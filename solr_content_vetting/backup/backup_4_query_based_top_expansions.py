import argparse
import logging
import sys

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, collect_set, concat_ws, row_number, count
from pyspark.sql.window import Window

from common import (
    s3_working_folder, format_date,
    remove_bad_expansion_UDF
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
    .appName("query_based_top_expansions")
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

capped_token_count_threshold = 50

@dataclass
class Thresholds:
    cap_score_threshold: float
    distinct_query_count_threshold: int
    total_clicks_threshold: int

# Keep top 15% of <ASIN, expansions>
marketplace_thresholds_15 = {
    # NA
    1: Thresholds(1.52, 10, 58),
}

# Keep top 20% of <ASIN, expansions>
marketplace_thresholds_20 = {
    # NA
    1: Thresholds(1.4242, 6, 31),
}

marketplace_thresholds_25 = {
    # NA
    1: Thresholds(0.102, 5, 25),
}

marketplace_thresholds_30 = {
    # NA
    1: Thresholds(0.0645, 4, 21),
}

marketplace_thresholds_40 = {
    # NA
    1: Thresholds(0.0368, 3, 15),
}

marketplace_thresholds_50 = {
    # NA
    1: Thresholds(0.0259, 3, 12),
}


def keep_query_based_top_expansions(query_based_expansion, marketplace_id, top_thresholds, s3_asin_top_expansion_set, s3_asin_top_expansion_tokens):
    # For each ASIN, keep at most capped_threshold click tokens, rank by click count
    w = Window().partitionBy("asin").orderBy(col("total_clicks").desc())
    asin_top_expansions = (
        query_based_expansion
            .where(col("cap_score") >= top_thresholds.cap_score_threshold)
            .where(col("distinct_query_count") >= top_thresholds.distinct_query_count_threshold)
            .where(col("total_clicks") >= top_thresholds.total_clicks_threshold)
    )

    asin_top_expansions_capped = (
        asin_top_expansions
            .withColumn("rn", row_number().over(w))
            .where(col("rn") <= capped_token_count_threshold)
            .select("asin", "expansion")
    )

    covered_asin = asin_top_expansions_capped.select("asin").distinct()
    logger.info(f"Covered ASIN count:{covered_asin.count()}")

    asin_top_expansion_set = (
        asin_top_expansions_capped
            .groupBy("asin")
            .agg(collect_set("expansion").alias("expansion_set"))
    )

    asin_top_expansion_set = asin_top_expansion_set.coalesce(100)
    asin_top_expansion_set.write.mode("overwrite").parquet(s3_asin_top_expansion_set)

    asin_top_expansion_tokens = (
        asin_top_expansion_set
            .withColumn("tokens", concat_ws(" ", col("expansion_set")))
            .select("asin", lit(marketplace_id).alias("marketplace_id"), "tokens")
    )
    asin_top_expansion_tokens = asin_top_expansion_tokens.coalesce(100)
    asin_top_expansion_tokens.write.mode("overwrite").parquet(s3_asin_top_expansion_tokens)


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")

    working_folder = f"{s3_working_folder}{region}/{marketplace_name}/solr_index_{solr_index_latest_date}/"

    # S3 Outputs
    s3_asin_top_expansion_set = f"{working_folder}query_based_asin_top_expansion_set/"
    s3_asin_top_expansion_tokens = f"{working_folder}query_based_asin_top_expansion_tokens/"

    # S3 Inputs
    s3_query_based_expansion = f"{working_folder}query_based_expansion/"
    query_based_expansion = spark.read.parquet(s3_query_based_expansion)
    query_based_expansion.show(10, False)
    logger.info(f"query_based_expansion <ASIN, expansion> count: {query_based_expansion.count()}")

    # Remove bad expansions
    query_based_expansion = (
        query_based_expansion
            .withColumn(
                "is_filtered",
                remove_bad_expansion_UDF(col("asin"), col("expansion"))
            )
            .where(col("is_filtered") == 0)
    )

    logger.info(f"query_based_expansion after bad expansions filtering <ASIN, expansion> count: {query_based_expansion.count()}")

    threshold = "threshold_15/"
    keep_query_based_top_expansions(query_based_expansion, args.marketplace_id, marketplace_thresholds_15[args.marketplace_id],
                                   s3_asin_top_expansion_set + threshold,
                                   s3_asin_top_expansion_tokens + threshold)

    threshold = "threshold_20/"
    keep_query_based_top_expansions(query_based_expansion, args.marketplace_id, marketplace_thresholds_20[args.marketplace_id],
                                   s3_asin_top_expansion_set + threshold,
                                   s3_asin_top_expansion_tokens + threshold)

    threshold = "threshold_25/"
    keep_query_based_top_expansions(query_based_expansion, args.marketplace_id, marketplace_thresholds_25[args.marketplace_id],
                                   s3_asin_top_expansion_set + threshold,
                                   s3_asin_top_expansion_tokens + threshold)

    threshold = "threshold_30/"
    keep_query_based_top_expansions(query_based_expansion, args.marketplace_id, marketplace_thresholds_30[args.marketplace_id],
                                   s3_asin_top_expansion_set + threshold,
                                   s3_asin_top_expansion_tokens + threshold)

    threshold = "threshold_40/"
    keep_query_based_top_expansions(query_based_expansion, args.marketplace_id, marketplace_thresholds_40[args.marketplace_id],
                                   s3_asin_top_expansion_set + threshold,
                                   s3_asin_top_expansion_tokens + threshold)

    threshold = "threshold_50/"
    keep_query_based_top_expansions(query_based_expansion, args.marketplace_id, marketplace_thresholds_50[args.marketplace_id],
                                   s3_asin_top_expansion_set + threshold,
                                   s3_asin_top_expansion_tokens + threshold)


if __name__ == "__main__":
    main()

