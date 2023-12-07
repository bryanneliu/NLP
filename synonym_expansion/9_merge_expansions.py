import argparse
import logging
import sys

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, collect_set, concat_ws, row_number
from pyspark.sql.window import Window

from utils import *
from expansion_udf import *

from marketplace_utils import (
    get_marketplace_name
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
    .appName("merge_expansions")
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


def load_synonym_expansion(s3_synonym_expansion):
    synonym_expansion = spark.read.parquet(s3_synonym_expansion)
    synonym_expansion.show(10, False)

    asin_expansion = synonym_expansion.select("asin", "expansion").drop_duplicates()
    logger.info(f"asin_expansion unique <ASIN, expansion> count: {asin_expansion.count()}")

    unique_asin_count = asin_expansion.select("asin").distinct().count()
    logger.info(f"asin_expansion unique ASIN count: {unique_asin_count}\n")

    return asin_expansion


@dataclass
class Thresholds:
    cap_score_threshold: float
    distinct_query_count_threshold: int
    total_clicks_threshold: int


def load_click_based_expansion(args, s3_clicked_based_expansion):
    # Read from S3
    clicked_based_expansion = spark.read.parquet(s3_clicked_based_expansion)
    clicked_based_expansion.show(10, False)
    logger.info(f"clicked_based_expansion <ASIN, expansion> count: {clicked_based_expansion.count()}")

    # Remove bad expansions
    clicked_based_expansion = (
        clicked_based_expansion
            .withColumn(
                "is_filtered",
                remove_bad_expansion_UDF(col("asin"), col("expansion"))
            )
    )
    clicked_based_expansion = clicked_based_expansion.where(col("is_filtered") == 0)
    logger.info(f"clicked_based_expansion <ASIN, expansion> count after removing bad expansions: {clicked_based_expansion.count()}\n")

    # Keep top 20% of <ASIN, expansions>, define thresholds
    marketplace_thresholds = {
        1: Thresholds(0.0747, 6, 20),
        7: Thresholds(0.0547, 5, 15),
    }
    top_thresholds = marketplace_thresholds[args.marketplace_id]
    capped_threshold = 30

    w = Window().partitionBy("asin").orderBy(col("total_clicks").desc())

    asin_expansion_strict = (
        clicked_based_expansion
            .where(col("cap_score") >= top_thresholds.cap_score_threshold)
            .where(col("distinct_query_count") >= top_thresholds.distinct_query_count_threshold)
            .where(col("total_clicks") >= top_thresholds.total_clicks_threshold)
    )

    asin_expansion_strict_capped = (
        asin_expansion_strict
            .withColumn("rn", row_number().over(w))
            .where(col("rn") <= capped_threshold)
            .select("asin", "expansion")
    )

    logger.info(f"asin_expansion_strict unique <ASIN, expansion> count: {asin_expansion_strict_capped.count()}")
    unique_asin_count = asin_expansion_strict_capped.select("asin").distinct().count()
    logger.info(f"asin_expansion_strict unique ASIN count: {unique_asin_count}\n")

    asin_expansion_loose = (
        clicked_based_expansion
            .where(col("distinct_query_count") >= top_thresholds.distinct_query_count_threshold)
            .where(col("total_clicks") >= top_thresholds.total_clicks_threshold)
    )

    asin_expansion_loose_capped = (
        asin_expansion_loose
            .withColumn("rn", row_number().over(w))
            .where(col("rn") <= capped_threshold)
            .select("asin", "expansion")
    )

    logger.info(f"asin_expansion_loose unique <ASIN, expansion> count: {asin_expansion_loose_capped.count()}")
    unique_asin_count = asin_expansion_loose_capped.select("asin").distinct().count()
    logger.info(f"asin_expansion_loose unique ASIN count: {unique_asin_count}\n")

    return asin_expansion_strict_capped, asin_expansion_loose_capped


def load_product_type_based_expansion(s3_product_type_based_expansion):
    product_type_based_expansion = spark.read.parquet(s3_product_type_based_expansion)
    logger.info(f"asin_expansion unique <ASIN, expansion> count: {product_type_based_expansion.count()}")

    unique_asin_count = product_type_based_expansion.select("asin").distinct().count()
    logger.info(f"asin_expansion unique ASIN count: {unique_asin_count}\n")

    return product_type_based_expansion


def load_color_based_expansion(s3_color_based_expansion):
    color_based_expansion = spark.read.parquet(s3_color_based_expansion)
    logger.info(f"asin_expansion unique <ASIN, expansion> count: {color_based_expansion.count()}")

    unique_asin_count = color_based_expansion.select("asin").distinct().count()
    logger.info(f"asin_expansion unique ASIN count: {unique_asin_count}\n")

    return color_based_expansion


'''
Merge all expansions:
1. Synonym expansion based on queries
2. Synonym expansion based on expressions
3. Clicked-based expansions
4. Product_type-based expansion
5. Color-based expansion
'''
def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")
    s3_working_folder = f"{s3_sp_not_matched_set_prefix}{marketplace_name}/solr_index_{solr_index_latest_date}/"

    # S3 Inputs
    s3_query_based_synonym_expansion = f"{s3_working_folder}reviewed_synonym_expansion/*/"
    s3_expression_based_synonym_expansion = f"{s3_working_folder}reviewed_synonym_expansion_for_expressions/"
    s3_clicked_based_expansion = f"{s3_working_folder}clicked_based_expansion/"
    s3_product_type_based_expansion = f"{s3_working_folder}product_type_based_expansion/"
    s3_color_based_expansion = f"{s3_working_folder}color_based_expansion/"

    # S3 Outputs
    s3_asin_expansion_merged_synonym = f"{s3_working_folder}asin_expansion_merged_synonym/"
    s3_asin_expansion_merged_all_no_click = f"{s3_working_folder}asin_expansion_merged_all_no_click/"
    s3_asin_expansion_merged_all = f"{s3_working_folder}asin_expansion_merged_all/"


    # Load each expansion source
    logger.info("######################################")
    logger.info("Load query-based synonym expansions:")
    logger.info("######################################")
    query_based_synonym_expansion = load_synonym_expansion(s3_query_based_synonym_expansion)

    logger.info("######################################")
    logger.info("Load expression-based synonym expansions:")
    logger.info("######################################")
    expression_based_synonym_expansion = load_synonym_expansion(s3_expression_based_synonym_expansion)

    logger.info("######################################")
    logger.info("Load click-based expansions:")
    logger.info("######################################")
    click_based_expansion_strict, click_based_expansion_loose = load_click_based_expansion(args, s3_clicked_based_expansion)

    logger.info("######################################")
    logger.info("Load product_type_based expansion:")
    logger.info("######################################")
    product_type_based_expansion = load_product_type_based_expansion(s3_product_type_based_expansion)

    logger.info("######################################")
    logger.info("Load color_based expansion:")
    logger.info("######################################")
    color_based_expansion = load_color_based_expansion(s3_color_based_expansion)


    '''
    Merge Synonym Expansion
    '''
    logger.info("##############################################")
    logger.info("Merge query & expression synonym expansions:")
    logger.info("##############################################")
    synonym_expansion = (
        query_based_synonym_expansion
            .union(expression_based_synonym_expansion)
            .drop_duplicates()
    )
    logger.info(f"synonym_expansion unique <ASIN, expansion> count: {synonym_expansion.count()}")

    synonym_expansion = (
        synonym_expansion
            .groupBy("asin")
            .agg(collect_set("expansion").alias("expansion_set"))
            .withColumn("tokens", concat_ws(" ", col("expansion_set")))
            .select("asin", lit(args.marketplace_id).alias("marketplace_id"), "tokens")
    )
    covered_unique_asin_count = synonym_expansion.count()
    logger.info(f"synonym_expansion covered unique ASIN count: {covered_unique_asin_count}\n")

    synonym_expansion = synonym_expansion.coalesce(100)
    synonym_expansion.write.mode("overwrite").parquet(s3_asin_expansion_merged_synonym)


    '''
    Merge Synonym Expansion & Pt/Color-based Expansion
    '''
    logger.info("#####################################################")
    logger.info("Merge Synonym Expansion & Pt/Color-based Expansion:")
    logger.info("#####################################################")
    asin_expansion_merged_all_no_click = (
        query_based_synonym_expansion
            .union(expression_based_synonym_expansion)
            .union(product_type_based_expansion)
            .union(color_based_expansion)
            .drop_duplicates()
    )
    logger.info(f"asin_expansion_merged_all_no_click unique <ASIN, expansion> count: {asin_expansion_merged_all_no_click.count()}")

    asin_expansion_merged_all_no_click = (
        asin_expansion_merged_all_no_click
            .groupBy("asin")
            .agg(collect_set("expansion").alias("expansion_set"))
            .withColumn("tokens", concat_ws(" ", col("expansion_set")))
            .select("asin", lit(args.marketplace_id).alias("marketplace_id"), "tokens")
    )
    covered_unique_asin_count = asin_expansion_merged_all_no_click.count()
    logger.info(f"asin_expansion_merged_all_no_click covered unique ASIN count: {covered_unique_asin_count}\n")

    asin_expansion_merged_all_no_click = asin_expansion_merged_all_no_click.coalesce(100)
    asin_expansion_merged_all_no_click.write.mode("overwrite").parquet(s3_asin_expansion_merged_all_no_click)


    '''
    Merge all sources: synonym & pt & color & click
    '''
    logger.info("##################################################")
    logger.info("Merge all sources: synonym & pt & color & click:")
    logger.info("##################################################")
    asin_expansion_merged_all = (
        query_based_synonym_expansion
            .union(expression_based_synonym_expansion)
            .union(click_based_expansion_strict)
            .union(product_type_based_expansion)
            .union(color_based_expansion)
            .drop_duplicates()
    )
    logger.info(f"asin_expansion_merged_all unique <ASIN, expansion> count: {asin_expansion_merged_all.count()}")

    asin_expansion_merged_all = (
        asin_expansion_merged_all
            .groupBy("asin")
            .agg(collect_set("expansion").alias("expansion_set"))
            .withColumn("tokens", concat_ws(" ", col("expansion_set")))
            .select("asin", lit(args.marketplace_id).alias("marketplace_id"), "tokens")
    )
    covered_unique_asin_count = asin_expansion_merged_all.count()
    logger.info(f"asin_expansion_merged_all covered unique ASIN count: {covered_unique_asin_count}\n")

    asin_expansion_merged_all = asin_expansion_merged_all.coalesce(100)
    asin_expansion_merged_all.write.mode("overwrite").parquet(s3_asin_expansion_merged_all)


if __name__ == "__main__":
    main()

