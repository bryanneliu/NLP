import argparse
import logging
import subprocess
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, collect_list, struct

from expansion_udf import (
    remove_matched_queries_UDF
)

from utils import (
    format_date, s3_path_exists,
    get_s3_match_set_prefix, get_s3_sp_not_matched_set_prefix
)

from marketplace_utils import (
    get_marketplace_name,
    get_region_name
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    #format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    format="%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.INFO,
)

spark = (
    SparkSession.builder.master("yarn")
    .appName("get_sp_not_matched_set")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext

def process_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--marketplace_id",
        type=int,
        default=1,
        help="Specify solr_index marketplaceid",
        required=True
    )

    parser.add_argument(
        "--solr_index_latest_ds",
        type=str,
        default="2022-12-11",
        help="Specify solr_index ds",
        required=True
    )

    parser.add_argument(
        "--top_n_impression_ASIN",
        type=int,
        default=100,
        help="Select top n impression ASIN",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


def load_solr_index(args, solr_index_latest_date):
    region = get_region_name(args.marketplace_id).upper()
    s3_solr_index = f"s3://spear-shared/dataset/solr_index/ds={solr_index_latest_date}/region={region}/"
    solr_index = spark.read.parquet(s3_solr_index)
    solr_index = solr_index.where(col("marketplaceid") == args.marketplace_id).select("asin", "title_tokens", "index_tokens")
    solr_index_asin = solr_index.select("asin")
    solr_index_asin.cache()
    logger.info(f"solr_index unique ASIN count: {solr_index_asin.count()}")
    return solr_index_asin, solr_index


def get_SP_not_matched_set(args, match_set, solr_index_asin, solr_index, s3_sp_not_matched_set):
    # Get top match set
    top_match_set = (
        match_set
        .where(col("pos") < args.top_n_impression_ASIN)
        .where(col("impressions") > 1)
    )

    # match_set join solr_index_asin to get sp_match_set
    sp_match_set_with_asin = (
        top_match_set
            .join(solr_index_asin.hint("shuffle_hash"), ["asin"], "inner")
            .withColumn("query_asin_impressions", col("impressions"))
            .withColumn("query_freq", col("total_freq"))
            .withColumn("min_pos", col("pos"))
            .drop("impressions")
            .drop("total_freq")
            .drop("pos")
    )

    # Group by ASIN to get aggregated query data
    sp_match_set_asin_level = (
        sp_match_set_with_asin
            .groupBy("asin")
            .agg(
                collect_list(
                    struct("normalized_query", "query_freq", "query_asin_impressions",
                           "min_pos", "clicks", "adds", "purchases", "consumes")
                ).alias("query_data")
            )
    )

    # Join solr_index to get ASIN title_tokens and index_tokens
    sp_match_set_with_index = sp_match_set_asin_level.join(solr_index.hint("shuffle_hash"), ["asin"], "inner")

    # Get SP <asin, not_matched_query_data>
    sp_not_matched_with_index = (
        sp_match_set_with_index
            .withColumn(
                "not_matched_query_data",
                remove_matched_queries_UDF(col("query_data"), col("index_tokens"))
            )
            .drop("query_data")
            .where(col("not_matched_query_data") != array())
    )

    sp_not_matched_with_index = sp_not_matched_with_index.coalesce(2000)
    sp_not_matched_with_index.write.mode("overwrite").parquet(s3_sp_not_matched_set)


def get_all_SP_not_matched_set(args):
    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()

    s3_match_set_prefix = get_s3_match_set_prefix(region)
    s3_sp_not_matched_set_prefix = get_s3_sp_not_matched_set_prefix(region)

    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")

    # Load solr_index
    solr_index_asin, solr_index = load_solr_index(args, solr_index_latest_date)

    # Load each match set
    s3_match_set_folder = f"{s3_match_set_prefix}{marketplace_name}/"
    ls_output = str(subprocess.check_output(['aws', 's3', 'ls', f"{s3_match_set_folder}"]).decode('UTF-8'))
    split_outputs = ls_output.strip().split()
    date_folders = sorted([date_folder for date_folder in split_outputs if 'PRE' not in date_folder])

    '''
    s3://synonym-expansion-experiments/asin_level_synonym_expansion/ca/20211016_20211031/
    s3://synonym-expansion-experiments/asin_level_synonym_expansion/ca/20211101_20211115/
    '''
    for date_folder in date_folders:
        s3_sp_not_matched_set = f"{s3_sp_not_matched_set_prefix}{marketplace_name}/solr_index_{solr_index_latest_date}/sp_not_matched_set/{date_folder}"
        if s3_path_exists(sc, s3_sp_not_matched_set):
            continue

        # s3://synonym-expansion-experiments/asin_level_synonym_expansion/ca/20211016_20211031/match_set/token_count=10/
        s3_match_set = f"{s3_match_set_folder}{date_folder}match_set/token_count=*/"
        match_set = spark.read.parquet(s3_match_set)

        get_SP_not_matched_set(args, match_set, solr_index_asin, solr_index, s3_sp_not_matched_set)


def main():
    # Initialization
    args = process_args()

    get_all_SP_not_matched_set(args)


if __name__ == "__main__":
    main()

'''
sp_not_matched_set:

root
 |-- asin: string (nullable = true)
 |-- index_tokens: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- not_matched_query_data: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- normalized_query: string (nullable = true)
 |    |    |-- query_freq: integer (nullable = true)
 |    |    |-- query_asin_impressions: integer (nullable = true)
 |    |    |-- min_pos: integer (nullable = true): in the recent date_folder (5 days of US, 14 days of CA), min_pos of <query, asin>
 |    |    |-- clicks: integer (nullable = true)
 |    |    |-- adds: integer (nullable = true)
 |    |    |-- purchases: integer (nullable = true)
 |    |    |-- consumes: integer (nullable = true)
 
|1533336881|[{networking building relationship, 3, 3, 36, 0, 0, 0, 0}, 
             {flying saucer serious business, 3, 2, 6, 0, 0, 0, 0}, 
             {family business y anna murdoch, 2, 2, 3, 0, 0, 0, 0}, 
             {michal berman, 12, 2, 28, 0, 0, 0, 0}, 
             {business continuity planning, 2, 2, 60, 0, 0, 0, 0}, 
             {family business jonathan sim, 31, 2, 6, 0, 0, 0, 0}, 
             {michal oron, 2, 2, 43, 0, 0, 0, 0}, 
             {anna murdoch book family business, 2, 2, 20, 0, 0, 0, 0}] 
'''