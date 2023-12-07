import argparse
import logging
import sys

from pyspark.sql import SparkSession, Column
from pyspark.sql.column import _to_seq, _to_java_column
from pyspark.sql.functions import col, lit, udf, expr

from common import *

from marketplace_utils import (
    get_marketplace_name
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    #format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    format="%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.INFO,
)

spark = (
    SparkSession.builder.master("yarn")
    .appName("tokenize_query_sets")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext

def process_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--marketplace_id",
        type=int,
        default=6,
        help="Specify marketplaceid",
        required=True
    )

    parser.add_argument(
        "--match_set_on_date",
        type=str,
        default="20221212",
        help="The date should be in this format and be used in tommy_asin",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


def solr5_tokenize(text, marketplace_id):
    sc = SparkSession.builder.getOrCreate()
    _solr5_tp = sc._jvm.com.amazon.productads.latac.udfs.Solr5AnalyzerPySparkUDF.solr5TokenizeUdf()
    return Column(_solr5_tp.apply(_to_seq(sc, [text, marketplace_id], _to_java_column)))


def normalize_query_set(s3_raw_query_set, s3_normalized_query_set):
    if s3_path_exists(sc, s3_normalized_query_set):
        logger.info(f"{s3_normalized_query_set} already exists, return")
        return

    raw_query_set = spark.read.parquet(s3_raw_query_set)
    normalized_query_set = (
        raw_query_set
           # .withColumn("normalized_query", solr5_tokenize(col("raw_query"), col("marketplace_id")))
            .withColumn("marketplace_id_str", col("marketplace_id").cast("string"))
            .withColumn("normalized_query", expr("analyzerUdf(raw_query, marketplace_id_str)"))
            .withColumn("query_token_count", get_query_token_count_UDF(col("normalized_query")))
            .where(col("query_token_count") <= 20)
            .drop("marketplace_id", "marketplace_id_str")
    )

    normalized_query_set.show(20, False)

    normalized_query_set.repartition(10).write.mode("overwrite").parquet(s3_normalized_query_set)

    logger.info("Writing normalized query sets to S3 is done.\n")

'''
Tokenize all raw query sets
'''
def main():
    # Initialization
    args = process_args()

    marketplace = get_marketplace_name(args.marketplace_id).upper()

    # Register Solr8 Tokenizer UDF
    spark.udf.registerJavaFunction("analyzerUdf", "com.amazon.productads.latac.udfs.Solr8LuceneAnalyzer")

    # Match set date format (2022-12-11)
    formatted_match_set_date = format_date(args.match_set_on_date, "%Y-%m-%d")
    s3_match_set_folder = f"{s3_match_set_prefix}{marketplace}/{formatted_match_set_date}/"

    s3_raw_query_set = s3_match_set_folder + "raw_query_set/"
    s3_normalized_query_set = s3_match_set_folder + "normalized_query_set/"

    # Normalize query sets
    logger.info("Daily normalized query set:")
    normalize_query_set(s3_raw_query_set + "daily", s3_normalized_query_set + "daily")

    logger.info("Daily tail normalized query set:")
    normalize_query_set(s3_raw_query_set + "daily_tail", s3_normalized_query_set + "daily_tail")

    logger.info("Segment normalized query set:")
    normalize_query_set(s3_raw_query_set + "segment", s3_normalized_query_set + "segment")


if __name__ == "__main__":
    main()
