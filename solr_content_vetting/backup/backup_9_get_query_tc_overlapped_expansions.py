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

def handle_null_array(arr):
    return arr if arr is not None else []

def calculate_overlap(marketplace_id, s3_query_based_expansion_set, s3_tc_based_expansion_set, s3_overlap, query_threshold, tc_threshold, s3_merged=""):
    logger.info(f"query_threshold:{query_threshold}, tc_threshold:{tc_threshold}")

    # Get <query, tc> overlap
    query_token_set = spark.read.parquet(f"{s3_query_based_expansion_set}{query_threshold}/")
    query_token_set = query_token_set.withColumnRenamed("expansion_set", "query_expansion_set")

    tc_token_set = spark.read.parquet(f"{s3_tc_based_expansion_set}{tc_threshold}/")
    tc_token_set = tc_token_set.withColumnRenamed("expansion_set", "tc_expansion_set")

    query_tc_token_set = query_token_set.join(tc_token_set, ["asin"], "inner")

    overlap = (
        query_tc_token_set
            .select(
                "asin",
                array_intersect(col("query_expansion_set"), col("tc_expansion_set")).alias("overlap_expansion_set")
            )
            .where(col("overlap_expansion_set") != array())
    )

    output = (
        overlap
            .withColumn("tokens", concat_ws(" ", col("overlap_expansion_set")))
            .select("asin", lit(marketplace_id).alias("marketplaceId"), "tokens")
    )
    output = output.coalesce(10)
    output.write.mode("overwrite").parquet(s3_overlap + f"query_{query_threshold}_tc_{tc_threshold}")

    if not s3_merged:
        return

    # Merge top query and <query, tc> overlap
    top_query_threshold = "threshold_30"
    query_token_set = spark.read.parquet(f"{s3_query_based_expansion_set}{top_query_threshold}/")
    query_token_set = query_token_set.withColumnRenamed("expansion_set", "query_expansion_set")

    # Contain the result of the full outer join, combining all rows from both DataFrames
    merged = (
        query_token_set
            .join(overlap, ["asin"], "full")
            .withColumn("merged_expansion_set",
                when(col("query_expansion_set").isNull(), col("overlap_expansion_set")).otherwise(
                    when(col("overlap_expansion_set").isNull(), col("query_expansion_set")).otherwise(
                        array_union(col("query_expansion_set"), col("overlap_expansion_set"))
                    )
                )
            )
            .select("asin", "merged_expansion_set")
            .where(col("merged_expansion_set") != array())
    )
    # array_union: The result is a new array that contains all unique elements from both input arrays
    # array_union expects non-null arrays as input.

    output = (
        merged
            .withColumn("tokens", concat_ws(" ", col("merged_expansion_set")))
            .select("asin", lit(marketplace_id).alias("marketplaceId"), "tokens")
    )
    output = output.coalesce(10)
    output.write.mode("overwrite").parquet(s3_merged + f"query_{query_threshold}_tc_{tc_threshold}")


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")

    working_folder = f"{s3_working_folder}{region}/{marketplace_name}/solr_index_{solr_index_latest_date}/"

    s3_query_based_expansion_set = f"{working_folder}query_based_asin_top_expansion_set/"
    s3_tc_based_expansion_set = f"{working_folder}tc_based_asin_top_expansion_set/"
    s3_overlap = f"{working_folder}query_tc_overlap/"
    s3_merged = f"{working_folder}merged/"

    query_threshold, tc_threshold = "threshold_50", "threshold_100"
    calculate_overlap(args.marketplace_id, s3_query_based_expansion_set, s3_tc_based_expansion_set, s3_overlap, query_threshold, tc_threshold, s3_merged)

    query_threshold, tc_threshold = "threshold_50", "threshold_40"
    calculate_overlap(args.marketplace_id, s3_query_based_expansion_set, s3_tc_based_expansion_set, s3_overlap, query_threshold, tc_threshold)

    query_threshold, tc_threshold = "threshold_50", "threshold_25"
    calculate_overlap(args.marketplace_id, s3_query_based_expansion_set, s3_tc_based_expansion_set, s3_overlap, query_threshold, tc_threshold)

    query_threshold, tc_threshold = "threshold_25", "threshold_100"
    calculate_overlap(args.marketplace_id, s3_query_based_expansion_set, s3_tc_based_expansion_set, s3_overlap, query_threshold, tc_threshold, s3_merged)

    query_threshold, tc_threshold = "threshold_25", "threshold_40"
    calculate_overlap(args.marketplace_id, s3_query_based_expansion_set, s3_tc_based_expansion_set, s3_overlap, query_threshold, tc_threshold)

    query_threshold, tc_threshold = "threshold_25", "threshold_25"
    calculate_overlap(args.marketplace_id, s3_query_based_expansion_set, s3_tc_based_expansion_set, s3_overlap, query_threshold, tc_threshold)


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

