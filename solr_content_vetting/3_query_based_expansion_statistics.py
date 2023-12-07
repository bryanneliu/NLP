import argparse
import logging
import sys

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, percentile_approx, countDistinct, sum, percent_rank, desc

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
    .appName("query_based_expansion_statistics")
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


def load_clicked_missing_tokens(s3_query_based_missing_tokens):
    missing_tokens = spark.read.parquet(s3_query_based_missing_tokens)

    clicked = (
        missing_tokens
            .where(col("query.clicks") > 1)
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


def calculate_feature_value_distribution(clicked_agg):
    distinct_query_count_distribution = (
        clicked_agg.select(percentile_approx("distinct_query_count", np.arange(0.95, 0.4, -0.05).tolist(), 1000))
    )
    logger.info("distinct_query_count_distribution:")
    distinct_query_count_distribution.show(5, False)

    total_clicks_distribution = (
        clicked_agg.select(percentile_approx("total_clicks", np.arange(0.95, 0.4, -0.05).tolist(), 1000))
    )
    logger.info("total_clicks_distribution:")
    total_clicks_distribution.show(5, False)

    cap_score_distribution = (
        clicked_agg.select(percentile_approx("cap_score", np.arange(0.95, 0.4, -0.05).tolist(), 1000))
    )
    logger.info("cap_score_distribution:")
    cap_score_distribution.show(5, False)


# The percent_rank() function returns a value between 0 and 1,
# where 0 represents the lowest rank (the first row in the ordered set),
# and 1 represents the highest rank (the last row in the ordered set).
def calculate_feature_value_percentile(clicked_agg, s3_query_based_expansion):
    order_by_cap_score = Window.orderBy(desc("cap_score"))
    order_by_distinct_query_count = Window.orderBy(desc("distinct_query_count"))
    order_by_total_clicks = Window.orderBy(desc("total_clicks"))

    asin_token_percentile = (
        clicked_agg
            .withColumn("cap_score_percentile", percent_rank().over(order_by_cap_score))
            .withColumn("distinct_query_count_percentile", percent_rank().over(order_by_distinct_query_count))
            .withColumn("total_clicks_percentile", percent_rank().over(order_by_total_clicks))
            .select("asin", "expansion", "distinct_query_count", "total_clicks", "cap_score",
                "distinct_query_count_percentile", "total_clicks_percentile", "cap_score_percentile")
    )

    asin_token_percentile.show(5, False)

    asin_token_percentile = asin_token_percentile.coalesce(500)
    asin_token_percentile.write.mode("overwrite").parquet(s3_query_based_expansion)


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")

    working_folder = f"{s3_working_folder}{region}/{marketplace_name}/solr_index_{solr_index_latest_date}/"
    s3_query_based_missing_tokens = f"{working_folder}query_based_missing_tokens/" # Input
    s3_query_based_expansion = f"{working_folder}query_based_expansion/" # Output

    if s3_path_exists(sc, s3_query_based_expansion):
        logger.info(f"{s3_query_based_expansion} exists, return")
        return

    clicked_agg = load_clicked_missing_tokens(s3_query_based_missing_tokens)
    # calculate_feature_value_distribution(clicked_agg) # debug
    calculate_feature_value_percentile(clicked_agg, s3_query_based_expansion)


if __name__ == "__main__":
    main()

'''
missing_tokens:
root
 |-- asin: string (nullable = true)
 |-- expansion: string (nullable = true)
 |-- query: struct (nullable = true)
 |    |-- normalized_query: string (nullable = true)
 |    |-- query_freq: integer (nullable = true)
 |    |-- query_asin_impressions: integer (nullable = true)
 |    |-- clicks: integer (nullable = true)
 |    |-- adds: integer (nullable = true)
 |    |-- purchases: integer (nullable = true)
 |    |-- consumes: integer (nullable = true)
 
+----------+----------+-----------------------------------------------------------+
|asin      |expansion |query                                                      |
+----------+----------+-----------------------------------------------------------+
|0744056977|royalty   |{royalty etiquette book, 1215, 2, 2, 0, 0, 0}              |
|0800729471|tag       |{love has price tag elisabeth elliot, 421, 1, 1, 0, 0, 0}  |
|0800729471|has       |{love has price tag elisabeth elliot, 421, 1, 1, 0, 0, 0}  |
|0800729471|price     |{love has price tag elisabeth elliot, 421, 1, 1, 0, 0, 0}  |
|0935216219|revell    |{practice presence god lawrence revell, 3, 2, 2, 0, 0, 0}  |
|0993925472|carolyn   |{helene hadsell carolyn wilman, 1, 2, 1, 1, 0, 0}          |
|0993925472|wilman    |{helene hadsell carolyn wilman, 1, 2, 1, 1, 0, 0}          |
|0997486430|classroom |{bucket filler classroom, 6439, 8, 1, 1, 0, 0}             |
|1085829480|bilingual |{bilingual spanish montessori material, 221, 1, 1, 0, 0, 0}|
|1085829480|montessori|{bilingual spanish montessori material, 221, 1, 1, 0, 0, 0}|
+----------+----------+-----------------------------------------------------------+

Rank in descending order.
The percent_rank() function returns a value between 0 and 1, where 0 represents the lowest rank (the first row in the ordered set), 
and 1 represents the highest rank (the last row in the ordered set).
+----------+---------+--------------------+-------------------------------+
|asin      |expansion|distinct_query_count|distinct_query_count_percentile|
+----------+---------+--------------------+-------------------------------+
|B0BQZ9K23L|handle   |2825                |1.5616544560795444E-6          |
|B0B3DXNSGF|deal     |1208                |1.5538666779521136E-5          |
|B08KTZ8249|2023     |1388                |1.0945927099633657E-5          |
|B098NLQNDG|shoe     |10188               |3.279064474707705E-8           |
|B0BXDGVMHS|brother  |601                 |8.061784952598561E-5           |
|B0BNX6K3T8|handle   |4577                |3.565982616244629E-7           |
|B0BPQ34HYJ|oz       |6560                |9.632251894453882E-8           |
|B0B3DW64VY|clearance|684                 |6.013189422024923E-5           |
|B09TMHDVLD|pokemon  |6033                |1.188660872081543E-7           |
|B0BPQ34HYJ|30       |4338                |4.139818899318477E-7           |
+----------+---------+--------------------+-------------------------------+
'''