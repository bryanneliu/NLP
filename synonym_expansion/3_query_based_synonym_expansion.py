import argparse
import logging
import subprocess
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, explode, collect_set, countDistinct, sum, max, collect_list, struct

from expansion_udf import (
    expand_queries_for_synonym_dict
)

from utils import (
    format_date, s3_path_exists, load_reviewed_synonym, load_jp_synonym,
    get_s3_sp_not_matched_set_prefix
)

from marketplace_utils import (
    get_marketplace_name,
    get_region_name
)

import collections

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
    .appName("query_based_synonym_expansion")
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

def query_based_expansion(args, synonym_dict):
    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()

    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")
    s3_sp_not_matched_set_prefix = get_s3_sp_not_matched_set_prefix(region)

    s3_working_folder = f"{s3_sp_not_matched_set_prefix}{marketplace_name}/solr_index_{solr_index_latest_date}/"
    s3_sp_not_matched_set_folder = f"{s3_working_folder}sp_not_matched_set/"
    s3_synonym_expansion_folder = f"{s3_working_folder}reviewed_synonym_expansion/"

    do_missing_token = False
    expansion_udf = expand_queries_for_synonym_dict(synonym_dict, do_missing_token)

    ls_output = str(subprocess.check_output(['aws', 's3', 'ls', f"{s3_sp_not_matched_set_folder}"]).decode('UTF-8'))
    split_outputs = ls_output.strip().split()
    date_folders = sorted([date_folder for date_folder in split_outputs if 'PRE' not in date_folder])

    for date_folder in date_folders:
        s3_sp_not_matched_set = f"{s3_sp_not_matched_set_folder}{date_folder}"
        s3_synonym_expansion = f"{s3_synonym_expansion_folder}{date_folder}"

        if s3_path_exists(sc, s3_synonym_expansion):
            logger.info(f"{s3_synonym_expansion} exists")
            continue

        sp_not_matched_with_index = spark.read.parquet(s3_sp_not_matched_set)
        expanded_sp_with_index = (
            sp_not_matched_with_index
                .withColumn(
                    "expansions",
                    expansion_udf(
                        col("not_matched_query_data"),
                        col("title_tokens"),
                        col("index_tokens")
                    )
                )
                .drop("not_matched_query_data", "index_tokens")
                .where(col("expansions") != array())
        )

        expanded_sp = (
            expanded_sp_with_index
                .select("asin", explode(col("expansions")))
                .select("asin", "col.expansion", "col.debug", "col.query")
        )

        synonym_expansion = expanded_sp.where(col("debug") != "")
        synonym_expansion_asin_count = synonym_expansion.select("asin").distinct().count()
        logger.info(f"Start to write reviewed synonym expansion to {s3_synonym_expansion}")
        logger.info(f"synonym_expansion_asin_count: {synonym_expansion_asin_count}")
        synonym_expansion = synonym_expansion.coalesce(500)
        synonym_expansion.write.mode("overwrite").parquet(s3_synonym_expansion)


def main():
    # Initialization
    args = process_args()

    if args.marketplace_id == 6:
        synonym_dict = load_jp_synonym(spark, logger)
    else:
        synonym_dict = load_reviewed_synonym(spark, logger)

    query_based_expansion(args, synonym_dict)

if __name__ == "__main__":
    main()
