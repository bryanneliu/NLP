import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_intersect, array_union, array, when, size, count, max, avg, sum, lit, concat_ws
import pyspark.sql.functions as F

from common import (
    s3_working_folder, format_date, s3_path_exists,
)

from marketplace_utils import (
    get_marketplace_name,
    get_region_name
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    # format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    format="%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.INFO,
)

spark = (
    SparkSession.builder.master("yarn")
    .appName("get_query_tc_overlapped_expansions")
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
        default="2022-12-19",
        help="Specify solr_index ds",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


def calculate_solr_index_asin_count(region, marketplace_id, solr_index_latest_date):
    s3_solr_index = f"s3://spear-shared/dataset/solr_index/ds={solr_index_latest_date}/region={region}/"
    solr_index = spark.read.parquet(s3_solr_index)

    solr_index_asin = solr_index.where(col("marketplaceid") == marketplace_id).select("asin")
    solr_index_asin_count = solr_index_asin.count()
    logger.info(f"solr_index unique ASIN count: {solr_index_asin_count}\n")


# Merge asin_expansion from different data sources
def merge_asin_expansion_rank(working_folder):
    # Input
    s3_query_based_expansion = f"{working_folder}query_based_expansion/"
    s3_tc_based_expansion = f"{working_folder}tc_based_expansion/"

    # Output
    s3_asin_expansion_rank = f"{working_folder}asin_expansion_rank/"

    #####################################################
    # query based expansion join with tc based expansion
    #####################################################
    # "asin", "expansion", "distinct_query_count", "total_clicks", "cap_score",
    # "distinct_query_count_percentile", "total_clicks_percentile", "cap_score_percentile")
    # "distinct_tc_count", "distinct_tc_count_percentile"
    query_based_expansion = spark.read.parquet(s3_query_based_expansion)
    tc_based_expansion = spark.read.parquet(s3_tc_based_expansion)

    asin_expansion_rank = query_based_expansion.join(tc_based_expansion, ["asin", "expansion"], "full")

    logger.info(f"asin_expansion_rank <asin, expansion> pair count: {asin_expansion_rank.count()}")
    unique_asins = asin_expansion_rank.select("asin").distinct()
    logger.info(f"asin_expansion_rank unique asin count: {unique_asins.count()}")
    asin_expansion_rank.show(20, False)

    asin_expansion_rank = asin_expansion_rank.coalesce(100)
    asin_expansion_rank.write.mode("overwrite").parquet(f"{s3_asin_expansion_rank}")


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")

    working_folder = f"{s3_working_folder}{region}/{marketplace_name}/solr_index_{solr_index_latest_date}/"

    calculate_solr_index_asin_count(region, args.marketplace_id, solr_index_latest_date)
    merge_asin_expansion_rank(working_folder)


if __name__ == "__main__":
    main()

###################
# Merge strategies
###################

# ** top query missing tokens
# ** query_tc overlapped tokens
# top tc missing tokens

# capped token count: 30 or 40 or 50
# top 25% query tokens + overlapped tc tokens
# top 30% query tokens + overlapped tc tokens

# Click-driven + OPS-driven
# top 25% query tokens + overlapped tc tokens + overlapped tc variations (synonym, lemma) + overlapped title variations (synonyms, lemma)

