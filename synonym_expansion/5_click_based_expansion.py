import argparse
import logging
import sys

import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, percentile_approx, countDistinct, sum, percent_rank
# from pyspark.sql.window import Window

from utils import (
    format_date, s3_path_exists,
    get_s3_sp_not_matched_set_prefix
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
    .appName("click_based_expansion")
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


def load_clicked_missing_tokens(s3_working_folder):
    s3_missing_token_expansion = f"{s3_working_folder}missing_token_expansion/*/"
    missing_tokens = spark.read.parquet(s3_missing_token_expansion)

    clicked = (
        missing_tokens
        .where(col("query.clicks") > 0)
        .select("asin", "expansion",
                "query.normalized_query", "query.query_freq", "query.query_asin_impressions",
                "query.clicks", "query.adds", "query.purchases", "query.consumes")
    )

    clicked_agg = clicked.groupBy("asin", "expansion").agg(
        countDistinct("normalized_query").alias("distinct_query_count"),
        sum("query_freq").alias("total_query_freq"),
        sum("query_asin_impressions").alias("total_query_asin_impressions"),
        sum("clicks").alias("total_clicks"),
        sum("adds").alias("total_adds"),
        sum("purchases").alias("total_purchases"),
        sum("consumes").alias("total_consumes")
    )

    clicked_agg = (
        clicked_agg
            .withColumn(
                "cap_score",
                col("total_clicks") * 0.0007 + col("total_adds") * 0.02 + col("total_purchases") * 1.4 + col("total_consumes") * 1.4
            )
            .where(col("distinct_query_count") > 1)
            .where(col("total_clicks") > 1)
            .where(col("total_query_asin_impressions") > 1)
    )

    logger.info(f"clicked_agg count: {clicked_agg.count()}")
    clicked_agg.show(5, False)

    return clicked_agg


def calculate_feature_value_distribution(clicked_agg, s3_clicked_based_expansion):
    distinct_query_count_distribution = (
        clicked_agg.select(percentile_approx("distinct_query_count", np.arange(0.95, 0.65, -0.05).tolist(), 1000))
    )
    logger.info("distinct_query_count_distribution:")
    distinct_query_count_distribution.show(5, False)

    total_clicks_distribution = (
        clicked_agg.select(percentile_approx("total_clicks", np.arange(0.95, 0.65, -0.05).tolist(), 1000))
    )
    logger.info("total_clicks_distribution:")
    total_clicks_distribution.show(5, False)

    cap_score_distribution = (
        clicked_agg.select(percentile_approx("cap_score", np.arange(0.95, 0.65, -0.05).tolist(), 1000))
    )
    logger.info("cap_score_distribution:")
    cap_score_distribution.show(5, False)

    clicked_agg = clicked_agg.coalesce(500)
    clicked_agg.write.mode("overwrite").parquet(s3_clicked_based_expansion)

    '''
    order_score = Window.orderBy(col("distinct_query_count"))
    distinct_query_count_percentile = (
        clicked_agg
        .select("asin", "expansion", "distinct_query_count")
        .withColumn("percentile", percent_rank().over(order_score))
    )
    logger.info("Samples of distinct_query_count_percentile >= 0.9:")
    distinct_query_count_percentile.where(col("percentile") >= 0.9).show(20, False)
    distinct_query_count_percentile = distinct_query_count_percentile.coalesce(500)
    distinct_query_count_percentile.write.mode("overwrite").parquet(s3_distinct_query_count_percentile)
    '''


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()

    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")
    s3_sp_not_matched_set_prefix = get_s3_sp_not_matched_set_prefix(region)

    s3_working_folder = f"{s3_sp_not_matched_set_prefix}{marketplace_name}/solr_index_{solr_index_latest_date}/"
    s3_clicked_based_expansion = f"{s3_working_folder}clicked_based_expansion/"

    if s3_path_exists(sc, s3_clicked_based_expansion):
        logger.info(f"{s3_clicked_based_expansion} exists, return")
        return

    clicked_agg = load_clicked_missing_tokens(s3_working_folder)
    calculate_feature_value_distribution(clicked_agg, s3_clicked_based_expansion)


if __name__ == "__main__":
    main()
