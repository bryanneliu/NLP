import argparse
import logging
import sys

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, collect_set, concat_ws, row_number, count
from pyspark.sql.window import Window

from utils import (
    format_date, get_s3_sp_not_matched_set_prefix
)

from expansion_udf import (
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

capped_token_count_threshold = 30

@dataclass
class Thresholds:
    cap_score_threshold: float
    distinct_query_count_threshold: int
    total_clicks_threshold: int

# Keep top 5% of <ASIN,expansion>
marketplace_thresholds_5 = {
    # EU
    35691: Thresholds(4.5515, 15, 111),
    44551: Thresholds(4.4858, 13, 104),
    623225021: Thresholds(6.0128, 20, 175),
    338801: Thresholds(7.6037, 11, 93),
    679831071: Thresholds(3.1542, 12, 79),
    712115121: Thresholds(2.9354, 8, 64),
    338851: Thresholds(4.3792, 15, 145),
    338811: Thresholds(3.1061, 15, 117),
    328451: Thresholds(4.3119, 11, 75),
    704403121: Thresholds(3.0536, 9, 71),
    44571: Thresholds(1.8814, 23, 181),
    5: Thresholds(3.1311, 16, 95),
    4: Thresholds(4.3787, 17, 122),
    3: Thresholds(4.37, 16, 99),

    # FE
    104444012: Thresholds(5.7968, 9, 68),
    6: Thresholds(2.9952, 16, 117),
    111172: Thresholds(7.7702, 8, 72),
}

marketplace_thresholds_10 = {
    # EU
    35691: Thresholds(2.8677, 9, 57),
    44551: Thresholds(2.8642, 8, 55),
    623225021: Thresholds(2.9592, 12, 86),
    338801: Thresholds(4.348, 7, 51),
    679831071: Thresholds(1.5785, 8, 45),
    712115121: Thresholds(1.4926, 6, 37),
    338851: Thresholds(1.6525, 9, 74),
    338811: Thresholds(1.5434, 10, 61),
    328451: Thresholds(2.8442, 7, 43),
    704403121: Thresholds(1.556, 6, 41),
    44571: Thresholds(1.4389, 13, 86),
    5: Thresholds(1.5479, 10, 52),
    4: Thresholds(2.847, 10, 63),
    3: Thresholds(2.8596, 10, 54),

    # FE
    104444012: Thresholds(2.8863, 6, 41),
    6: Thresholds(1.501, 10, 62),
    111172: Thresholds(4.3512, 6, 41),
}

# Keep top 15% of <ASIN, expansions>, with at least 1 purchase/consume. Define thresholds
marketplace_thresholds_15 = {
    # NA
    771770: Thresholds(1.4946, 6, 49),
    526970: Thresholds(3.04, 8, 63),

    # EU
    35691: Thresholds(1.5024, 7, 38),
    44551: Thresholds(1.4982, 6, 37),
    623225021: Thresholds(1.5606, 9, 55),
    338801: Thresholds(2.9077, 6, 35),
    679831071: Thresholds(1.4582, 6, 31),
    712115121: Thresholds(1.4442, 4, 27),
    338851: Thresholds(1.4617, 7, 49),
    338811: Thresholds(1.4463, 7, 41),
    328451: Thresholds(1.4684, 6, 30),
    704403121: Thresholds(1.4561, 5, 29),
    44571: Thresholds(0.1715, 9, 54),
    5: Thresholds(1.4519, 7, 35),
    4: Thresholds(1.4718, 7, 42),
    3: Thresholds(1.4782, 7, 37),

    # FE
    104444012: Thresholds(1.4813, 5, 30),
    6: Thresholds(1.4382, 7, 42),
    111172: Thresholds(2.9098, 5, 29),
}

# Keep top 20% of <ASIN, expansions>, define thresholds
marketplace_thresholds_20 = {
    # NA
    1: Thresholds(0.0747, 6, 20),
    7: Thresholds(0.0547, 5, 15),
    771770: Thresholds(1.427, 5, 35),
    526970: Thresholds(1.761, 6, 45),

    # EU
    35691: Thresholds(1.4463, 6, 28),
    44551: Thresholds(1.4456, 5, 28),
    623225021: Thresholds(1.4649, 7, 39),
    338801: Thresholds(1.5705, 5, 27),
    679831071: Thresholds(1.4284, 5, 24),
    712115121: Thresholds(1.4228, 4, 21),
    338851: Thresholds(1.4228, 6, 36),
    338811: Thresholds(0.2241, 6, 30),
    328451: Thresholds(1.434, 5, 24),
    704403121: Thresholds(1.427, 4, 23),
    44571: Thresholds(0.0918, 7, 39),
    5: Thresholds(1.4256, 6, 27),
    4: Thresholds(1.4326, 6, 31),
    3: Thresholds(1.4435, 6, 28),

    # FE
    104444012: Thresholds(1.4361, 4, 23),
    6: Thresholds(0.1687, 6, 31),
    111172: Thresholds(1.6445, 4, 22),
}


def generate_click_based_expansion(args, clicked_based_expansion, top_thresholds, s3_asin_expansion_with_threshold):
    # For each ASIN, keep at most capped_threshold click tokens, rank by click count
    w = Window().partitionBy("asin").orderBy(col("total_clicks").desc())
    asin_expansion = (
        clicked_based_expansion
        .where(col("cap_score") >= top_thresholds.cap_score_threshold)
        .where(col("distinct_query_count") >= top_thresholds.distinct_query_count_threshold)
        .where(col("total_clicks") >= top_thresholds.total_clicks_threshold)
    )

    asin_expansion_capped = (
        asin_expansion
        .withColumn("rn", row_number().over(w))
        .where(col("rn") <= capped_token_count_threshold)
        .select("asin", "expansion")
    )

    logger.info(f"asin_expansion_capped unique <ASIN, expansion> count: {asin_expansion_capped.count()}")
    unique_asin_count = asin_expansion_capped.select("asin").distinct().count()
    logger.info(f"asin_expansion_capped unique ASIN count: {unique_asin_count}\n")

    asin_expansion_final = (
        asin_expansion_capped
        .groupBy("asin")
        .agg(collect_set("expansion").alias("expansion_set"))
        .withColumn("tokens", concat_ws(" ", col("expansion_set")))
        .select("asin", lit(args.marketplace_id).alias("marketplace_id"), "tokens")
    )

    covered_unique_asin_count = asin_expansion_final.count()
    logger.info(f"asin_expansion_final covered unique ASIN count: {covered_unique_asin_count}\n")

    asin_expansion_final = asin_expansion_final.coalesce(100)
    asin_expansion_final.write.mode("overwrite").parquet(s3_asin_expansion_with_threshold)


'''
Merge all expansions:
1. Synonym expansion based on queries (ignore)
2. Synonym expansion based on expressions (ignore)
3. Clicked-based expansions (Supported only)
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
    s3_clicked_based_expansion = f"{s3_working_folder}clicked_based_expansion/"

    # S3 Outputs
    s3_asin_expansion = f"{s3_working_folder}asin_expansion/"

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
    logger.info(
        f"clicked_based_expansion <ASIN, expansion> count after removing bad expansions: {clicked_based_expansion.count()}\n")

    generate_click_based_expansion(args, clicked_based_expansion, marketplace_thresholds_5[args.marketplace_id],
                                   s3_asin_expansion + "threshold_5/")
    generate_click_based_expansion(args, clicked_based_expansion, marketplace_thresholds_10[args.marketplace_id],
                                   s3_asin_expansion + "threshold_10/")
    generate_click_based_expansion(args, clicked_based_expansion, marketplace_thresholds_15[args.marketplace_id],
                                   s3_asin_expansion + "threshold_15/")
    generate_click_based_expansion(args, clicked_based_expansion, marketplace_thresholds_20[args.marketplace_id],
                                   s3_asin_expansion + "threshold_20/")


if __name__ == "__main__":
    main()

