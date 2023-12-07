import argparse
import logging
import sys

import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, percentile_approx, countDistinct, desc, percent_rank

from common import (
    s3_working_folder, format_date, s3_path_exists
)

from marketplace_utils import (
    get_marketplace_name, get_region_name
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
    .appName("tc_based_expansion_statistics")
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


def aggregate_asin_expansion(s3_tc_based_missing_tokens):
    missing_tokens = spark.read.parquet(s3_tc_based_missing_tokens)

    # missing_tokens: "asin", "col.expansion", "col.tc"
    asin_expansion_agg = (
        missing_tokens
            .groupBy("asin", "expansion")
            .agg(countDistinct("tc").alias("distinct_tc_count"))
    )

    logger.info(f"asin_expansion_agg count: {asin_expansion_agg.count()}")
    asin_expansion_agg.show(5, False)

    return asin_expansion_agg


def calculate_feature_value_distribution(asin_expansion_agg):
    distinct_tc_count_distribution = (
        asin_expansion_agg.select(percentile_approx("distinct_tc_count", np.arange(0.95, 0.4, -0.05).tolist(), 1000))
    )
    logger.info("distinct_tc_count_distribution:")
    distinct_tc_count_distribution.show(5, False)


def calculate_feature_value_percentile(asin_expansion_agg, s3_tc_based_expansion):
    order_by_distinct_tc_count = Window.orderBy(desc("distinct_tc_count"))
    asin_token_percentile = (
        asin_expansion_agg
            .withColumn("distinct_tc_count_percentile", percent_rank().over(order_by_distinct_tc_count))
            .select("asin", "expansion", "distinct_tc_count", "distinct_tc_count_percentile")
    )

    asin_token_percentile.show(5, False)

    asin_token_percentile = asin_token_percentile.coalesce(500)
    asin_token_percentile.write.mode("overwrite").parquet(s3_tc_based_expansion)


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")

    working_folder = f"{s3_working_folder}{region}/{marketplace_name}/solr_index_{solr_index_latest_date}/"
    s3_tc_based_missing_tokens = f"{working_folder}tc_based_missing_tokens/"
    s3_tc_based_expansion = f"{working_folder}tc_based_expansion/"

    if s3_path_exists(sc, s3_tc_based_expansion):
        logger.info(f"{s3_tc_based_expansion} exists, return")
        return

    asin_expansion_agg = aggregate_asin_expansion(s3_tc_based_missing_tokens)
    # calculate_feature_value_distribution(asin_expansion_agg) # debug
    calculate_feature_value_percentile(asin_expansion_agg, s3_tc_based_expansion)


if __name__ == "__main__":
    main()
