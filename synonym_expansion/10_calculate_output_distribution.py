import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, max, avg, sum, count
import pyspark.sql.functions as F

from utils import *
from expansion_udf import *

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
    .appName("calculate_output_distribution")
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


def output_csv(s3_asin_expansion_merged, s3_asin_expansion_merged_csv):
    asin_expansion_merged = spark.read.parquet(s3_asin_expansion_merged).coalesce(5)
    asin_expansion_merged.write.mode("overwrite").option("delimiter", "\t").csv(s3_asin_expansion_merged_csv)

    test = spark.read.csv(s3_asin_expansion_merged_csv)
    test.show(10, False)


def calculate_token_distribution(data, condition):
    data_with_condition = data.where(condition)
    data_count = data.count()
    data_with_condition_count = data_with_condition.count()

    logger.info(f"{condition}: {data_with_condition_count}, {round(data_with_condition_count / data_count, 4)}")


def calculate_output_distribution(s3_asin_expansion_merged, solr_index_asin_count):
    asin_expansion_merged = spark.read.parquet(s3_asin_expansion_merged)

    data = asin_expansion_merged.withColumn("addedTokenCount", size(split(col("tokens"), " ")))
    data.cache()

    calculate_token_distribution(data, col("addedTokenCount") == 1)
    calculate_token_distribution(data, (col("addedTokenCount") > 1) & (col("addedTokenCount") <= 5))
    calculate_token_distribution(data, (col("addedTokenCount") > 5) & (col("addedTokenCount") <= 10))
    calculate_token_distribution(data, (col("addedTokenCount") > 10) & (col("addedTokenCount") <= 20))
    calculate_token_distribution(data, (col("addedTokenCount") > 20) & (col("addedTokenCount") <= 30))
    calculate_token_distribution(data, col("addedTokenCount") > 30)
    calculate_token_distribution(data, col("addedTokenCount") > 50)
    calculate_token_distribution(data, col("addedTokenCount") > 100)

    addedTokenCount = data.agg(
        count("asin").alias("covered_asin_count"),
        F.round(count("asin")/solr_index_asin_count, 4).alias("covered_asin_coverage"),
        max("addedTokenCount").alias("max_added_tokens"),
        avg("addedTokenCount").alias("avg_added_tokens_for_covered_asins"),
        sum("addedTokenCount").alias("total_added_tokens")
    )
    addedTokenCount.show(5, False)


def calculate_solr_index_asin_count(args, solr_index_latest_date):
    region = get_region_name(args.marketplace_id).upper()
    s3_solr_index = f"s3://spear-shared/dataset/solr_index/ds={solr_index_latest_date}/region={region}/"
    solr_index = spark.read.parquet(s3_solr_index)

    solr_index_asin = solr_index.where(col("marketplaceid") == args.marketplace_id).select("asin")
    solr_index_asin_count = solr_index_asin.count()
    logger.info(f"solr_index unique ASIN count: {solr_index_asin_count}\n")

    return solr_index_asin_count


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")
    s3_working_folder = f"{s3_sp_not_matched_set_prefix}{marketplace_name}/solr_index_{solr_index_latest_date}/"

    # S3 Inputs
    s3_asin_expansion_merged_synonym = f"{s3_working_folder}asin_expansion_merged_synonym/"
    s3_asin_expansion_merged_all_no_click = f"{s3_working_folder}asin_expansion_merged_all_no_click/"
    s3_asin_expansion_merged_all = f"{s3_working_folder}asin_expansion_merged_all/"

    logger.info("########################################")
    logger.info("Calculate solr_index unique ASIN count:")
    logger.info("########################################")
    solr_index_asin_count = calculate_solr_index_asin_count(args, solr_index_latest_date)

    logger.info("#################################################################")
    logger.info("Calculate output distribution for synonym expansion:")
    logger.info("#################################################################")
    calculate_output_distribution(s3_asin_expansion_merged_synonym, solr_index_asin_count)

    logger.info("###################################################################")
    logger.info("Calculate output distribution for synonym & pt & color expansions:")
    logger.info("###################################################################")
    calculate_output_distribution(s3_asin_expansion_merged_all_no_click, solr_index_asin_count)

    logger.info("###########################################################################")
    logger.info("Calculate output distribution for synonym & pt & color & click expansions:")
    logger.info("###########################################################################")
    calculate_output_distribution(s3_asin_expansion_merged_all, solr_index_asin_count)


if __name__ == "__main__":
    main()

