import argparse
import logging
import sys

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, collect_set, concat_ws, row_number, count
from pyspark.sql.window import Window

from utils import (
    format_date,
    get_s3_sp_not_matched_set_prefix
)

from expansion_udf import (
    remove_bad_expansion_UDF
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


    # Keep top 15% of <ASIN, expansions>, with at least 1 purchase/consume. Define thresholds
    marketplace_thresholds_15 = {
        6: Thresholds(1.4221, 7, 31)
    }
    top_thresholds_15 = marketplace_thresholds_15[args.marketplace_id]

    # Keep top 20% of <ASIN, expansions>, define thresholds
    marketplace_thresholds_20 = {
        1: Thresholds(0.0747, 6, 20),
        7: Thresholds(0.0547, 5, 15),
        6: Thresholds(0.0662, 5, 23)
    }
    top_thresholds_20 = marketplace_thresholds_20[args.marketplace_id]


    # For each ASIN, keep at most capped_threshold click tokens, rank by click count
    capped_threshold = 30
    w = Window().partitionBy("asin").orderBy(col("total_clicks").desc())


    asin_expansion_strict = (
        clicked_based_expansion
            .where(col("cap_score") >= top_thresholds_15.cap_score_threshold)
            .where(col("distinct_query_count") >= top_thresholds_15.distinct_query_count_threshold)
            .where(col("total_clicks") >= top_thresholds_15.total_clicks_threshold)
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
            .where(col("cap_score") >= top_thresholds_20.cap_score_threshold)
            .where(col("distinct_query_count") >= top_thresholds_20.distinct_query_count_threshold)
            .where(col("total_clicks") >= top_thresholds_20.total_clicks_threshold)
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



'''
Merge all expansions:
1. Synonym expansion based on queries
2. Synonym expansion based on expressions
3. Clicked-based expansions
'''
def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()

    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")
    s3_sp_not_matched_set_prefix = get_s3_sp_not_matched_set_prefix(region)

    s3_working_folder = f"{s3_sp_not_matched_set_prefix}{marketplace_name}/solr_index_{solr_index_latest_date}/"

    # S3 Inputs
    s3_query_based_synonym_expansion = f"{s3_working_folder}reviewed_synonym_expansion/*/"
    s3_expression_based_synonym_expansion = f"{s3_working_folder}reviewed_synonym_expansion_for_expressions/"
    s3_clicked_based_expansion = f"{s3_working_folder}clicked_based_expansion/"

    # S3 Outputs
    s3_asin_expansion_merged_synonym = f"{s3_working_folder}asin_expansion_merged_synonym/"
    s3_asin_expansion_click_strict = f"{s3_working_folder}asin_expansion_click_strict/"
    s3_asin_expansion_click_loose = f"{s3_working_folder}asin_expansion_click_loose/"
    s3_asin_expansion_merged_strict = f"{s3_working_folder}asin_expansion_merged_strict/"
    s3_asin_expansion_merged_loose = f"{s3_working_folder}asin_expansion_merged_loose/"


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


    '''
    Merge Synonym Expansion
    '''
    logger.info("##############################################")
    logger.info("Merge query & expression synonym expansions:")
    logger.info("##############################################")

    synonym_expansion = (
        query_based_synonym_expansion
            .union(expression_based_synonym_expansion)
    )

    logger.info(f"synonym_expansion unique <ASIN, expansion> count: {synonym_expansion.count()}")

    synonym_expansion = (
        synonym_expansion
            .groupBy("asin", "expansion")
            .agg(count("*").alias("count"))
    )

    capped_threshold = 30
    w = Window().partitionBy("asin").orderBy(col("count").desc())

    synonym_expansion = (
        synonym_expansion
            .withColumn("rn", row_number().over(w))
            .where(col("rn") <= capped_threshold)
            .select("asin", "expansion")
    )

    synonym_expansion_final = (
        synonym_expansion
            .groupBy("asin")
            .agg(collect_set("expansion").alias("expansion_set"))
            .withColumn("tokens", concat_ws(" ", col("expansion_set")))
            .select("asin", lit(args.marketplace_id).alias("marketplace_id"), "tokens")
    )

    covered_unique_asin_count = synonym_expansion_final.count()
    logger.info(f"synonym_expansion covered unique ASIN count: {covered_unique_asin_count}\n")

    synonym_expansion_final = synonym_expansion_final.coalesce(100)
    synonym_expansion_final.write.mode("overwrite").parquet(s3_asin_expansion_merged_synonym)


    '''
    Strict Click-based Expansion
    '''
    asin_expansion_click_strict = (
        click_based_expansion_strict
            .groupBy("asin")
            .agg(collect_set("expansion").alias("expansion_set"))
            .withColumn("tokens", concat_ws(" ", col("expansion_set")))
            .select("asin", lit(args.marketplace_id).alias("marketplace_id"), "tokens")
    )

    covered_unique_asin_count = asin_expansion_click_strict.count()
    logger.info(f"asin_expansion_click_strict covered unique ASIN count: {covered_unique_asin_count}\n")

    asin_expansion_click_strict = asin_expansion_click_strict.coalesce(100)
    asin_expansion_click_strict.write.mode("overwrite").parquet(s3_asin_expansion_click_strict)


    '''
    Loose Click-based Expansion
    '''
    asin_expansion_click_loose = (
        click_based_expansion_loose
            .groupBy("asin")
            .agg(collect_set("expansion").alias("expansion_set"))
            .withColumn("tokens", concat_ws(" ", col("expansion_set")))
            .select("asin", lit(args.marketplace_id).alias("marketplace_id"), "tokens")
    )

    covered_unique_asin_count = asin_expansion_click_loose.count()
    logger.info(f"asin_expansion_click_loose covered unique ASIN count: {covered_unique_asin_count}\n")

    asin_expansion_click_loose = asin_expansion_click_loose.coalesce(100)
    asin_expansion_click_loose.write.mode("overwrite").parquet(s3_asin_expansion_click_loose)


    '''
    Merge Synonym Expansion & Strict Click-based Expansion
    '''
    logger.info("#####################################################")
    logger.info("Merge Synonym Expansion & Strict click-based Expansion:")
    logger.info("#####################################################")

    asin_expansion_merged_strict = (
        click_based_expansion_strict
            .select("asin", "expansion")
            .union(synonym_expansion)
    )
    
    logger.info(f"asin_expansion_merged_strict unique <ASIN, expansion> count: {asin_expansion_merged_strict.count()}")

    asin_expansion_merged_strict = (
        asin_expansion_merged_strict
            .groupBy("asin")
            .agg(collect_set("expansion").alias("expansion_set"))
            .withColumn("tokens", concat_ws(" ", col("expansion_set")))
            .select("asin", lit(args.marketplace_id).alias("marketplace_id"), "tokens")
    )

    covered_unique_asin_count = asin_expansion_merged_strict.count()
    logger.info(f"asin_expansion_merged_strict covered unique ASIN count: {covered_unique_asin_count}\n")

    asin_expansion_merged_strict = asin_expansion_merged_strict.coalesce(100)
    asin_expansion_merged_strict.write.mode("overwrite").parquet(s3_asin_expansion_merged_strict)


    '''
    Merge Synonyms Expansions & Loose Click-based Expansion 
    '''
    logger.info("##################################################")
    logger.info("Merge Synonym Expansion & Loose click-based Expansion:")
    logger.info("##################################################")

    asin_expansion_merged_loose = (
        click_based_expansion_loose
            .select("asin", "expansion")
            .union(synonym_expansion)
    )

    logger.info(f"asin_expansion_merged_loose unique <ASIN, expansion> count: {asin_expansion_merged_loose.count()}")

    asin_expansion_merged_loose = (
        asin_expansion_merged_loose
            .groupBy("asin")
            .agg(collect_set("expansion").alias("expansion_set"))
            .withColumn("tokens", concat_ws(" ", col("expansion_set")))
            .select("asin", lit(args.marketplace_id).alias("marketplace_id"), "tokens")
    )

    covered_unique_asin_count = asin_expansion_merged_loose.count()
    logger.info(f"asin_expansion_merged_loose covered unique ASIN count: {covered_unique_asin_count}\n")

    asin_expansion_merged_loose = asin_expansion_merged_loose.coalesce(100)
    asin_expansion_merged_loose.write.mode("overwrite").parquet(s3_asin_expansion_merged_loose)


if __name__ == "__main__":
    main()

