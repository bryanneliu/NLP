import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from pyspark.sql.functions import isnan, lower, regexp_replace, trim, when

from utils import *
from expansion_udf import (
    color_to_add_UDF
)

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
    .appName("Color_based_expansion")
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


def load_missing_tokens(s3_working_folder):
    s3_missing_token_expansion = f"{s3_working_folder}missing_token_expansion/*/"
    missing_tokens = spark.read.parquet(s3_missing_token_expansion)
    missing_tokens.printSchema()

    missing_tokens = (
        missing_tokens
            .groupBy("asin", "expansion")
            .agg(count("*").alias("query_count"))
            .drop("query_count")
    )

    covered_unique_asin_count = missing_tokens.select("asin").distinct().count()
    logger.info(f"covered_unique_asin_count: {covered_unique_asin_count}")

    return missing_tokens


# Replace all punctuations to space
def normalize_text(dataframe, column, normalized_column):
    return (
        dataframe
            .withColumn(
                normalized_column,
                trim(
                    regexp_replace(col(column), "[\s\p{Punct}]+", " ")
                )
            )
            .drop(column)
    )


def load_asin_normalized_color(args, solr_index_latest_date):
    janus = spark.sql(f"""
        SELECT DISTINCT asin, lower(color) AS color
        FROM default.janus
        WHERE marketplaceid = {args.marketplace_id} AND ds = '{solr_index_latest_date}' AND color is not null
    """)

    asin_normalized_color = normalize_text(janus, "color", "normalized_color")
    return asin_normalized_color


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")
    s3_working_folder = f"{s3_sp_not_matched_set_prefix}{marketplace_name}/solr_index_{solr_index_latest_date}/"

    s3_color_based_expansion = f"{s3_working_folder}color_based_expansion/"

    if s3_path_exists(sc, s3_color_based_expansion):
        logger.info(f"{s3_color_based_expansion} exists, return")
        return

    logger.info("Load ASIN normalized color:")
    asin_normalized_color = load_asin_normalized_color(args, solr_index_latest_date)

    logger.info("Load missing tokens:")
    missing_tokens = load_missing_tokens(s3_working_folder)

    logger.info("Join <asin, expansion> with <asin, normalized_color>:")
    asin_expansion_color = (
        missing_tokens.join(asin_normalized_color, ["asin"], "inner")
    )
    asin_expansion_color.show(5, False)

    logger.info("Color as an expansion:")
    asin_expansion = (
        asin_expansion_color
            .withColumn("color_to_add", color_to_add_UDF(col("expansion"), col("normalized_color")))
            .where(col("color_to_add") == 1)
            .select("asin", "expansion")
            .drop_duplicates()
    )

    logger.info(f"asin_expansion count: {asin_expansion.count()}")
    asin_expansion.show(10, False)

    asin_expansion = asin_expansion.coalesce(100)
    asin_expansion.write.mode("overwrite").parquet(s3_color_based_expansion)


if __name__ == "__main__":
    main()
