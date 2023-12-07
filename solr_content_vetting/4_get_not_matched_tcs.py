import argparse
import logging
import sys
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, collect_list, struct, expr, sum, min, udf, explode, max

from s3_with_nkw_utils import SnapshotMeta

from common import (
    s3_working_folder,
    format_date, s3_path_exists,
    remove_matched_tcs_UDF,
)

from marketplace_utils import (
    get_marketplace_name, get_janus_bucket, get_region_name, get_region_realm
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
    .appName("get_not_matched_tcs")
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


def load_solr_index(marketplace_id, s3_solr_index):
    solr_index = spark.read.parquet(s3_solr_index)
    solr_index = (
        solr_index
            .where(col("marketplaceid") == marketplace_id)
            .select("asin", "title_tokens", "index_tokens")
    )
    logger.info(f"solr_index unique ASIN count: {solr_index.count()}")
    return solr_index


# Get the latest snapshot of a day
def get_janus_latest_snapshot_mt_data(marketplace_name):
    janus_qdf_bucket = "spear-offline-data-prod"
    janus_qdf_prefix = "dataset/janus_index_nkw"

    meta = SnapshotMeta()
    janus_prefix = meta.get_snapshot_prefix(
        bucket=janus_qdf_bucket,
        meta_prefix=f"{janus_qdf_prefix}/marketplace={marketplace_name}/",
    )

    logger.info(f"Obtained Janus prefix from latest snapshot meta at {janus_prefix}")
    logger.info(f"Reading this latest meta ")

    janus_full_path = f"s3://{janus_qdf_bucket}/{janus_prefix}"
    janus_mt = spark.read.parquet(f"{janus_full_path}/janus_mt_data/")
    janus_mt = janus_mt.select("asin", "expression").distinct()

    return janus_mt


def get_janus_one_day_mt_data(marketplace_id, solr_index_latest_date, s3_head_asins):
    janus_date_folder = format_date(solr_index_latest_date, "%Y/%m/%d")
    janus = spark.read.json(f"s3://{get_janus_bucket(marketplace_id)}/{get_region_realm(marketplace_id)}/{janus_date_folder}/*/update*.gz")

    janus_mt = (
        janus
            .where(col("marketplaceid") == marketplace_id)
            .where(col("isForwardSearchable").isNull())  # MT
            .select("asin", "marketplaceid", "reviewCount", explode("clause"))
    )

    head_asins = (
        janus_mt
            .groupBy("asin")
            .agg(max("reviewCount").alias("reviewCount"))
            .where(col("reviewCount") >= 100)
            .select("asin")
    )
    head_asins = head_asins.coalesce(100)
    head_asins.write.mode("overwrite").parquet(s3_head_asins)

    mt_asin_expressions = janus_mt.select("asin", "col.expression").distinct()
    return mt_asin_expressions


# Return <asin, [normalized_tc]>
def get_asin_normalized_tcs(marketplace_id, asin_tcs):
    unique_expressions = asin_tcs.select("expression").distinct()
    analyzed_expressions = (
        unique_expressions
            .withColumn(
                "normalized_expression",
                expr(f"analyzerUdf(expression, '{marketplace_id}')"))
    )

    asin_normalized_tcs = (
        asin_tcs
            .hint("shuffle_hash")
            .join(analyzed_expressions, ["expression"], "inner")
            .groupBy("asin").agg(
                collect_list("normalized_expression").alias("tc_data")
            )
    )

    return asin_normalized_tcs


def get_not_matched_tcs(asin_normalized_tcs, solr_index, s3_not_matched_tcs):
    join_with_solr_index = (
        asin_normalized_tcs
            .join(solr_index, ["asin"], "inner")
    )

    # Get SP <asin, index, not_matched_tcs>
    not_matched_tcs_with_index = (
        join_with_solr_index
            .withColumn(
                "not_matched_tc_data",
                remove_matched_tcs_UDF(col("tc_data"), col("index_tokens"))
            )
            .drop("tc_data")
            .where(col("not_matched_tc_data") != array())
    )

    not_matched_tcs_with_index = not_matched_tcs_with_index.coalesce(2000)
    not_matched_tcs_with_index.write.mode("overwrite").parquet(s3_not_matched_tcs)


def register_solr8_tokenizer():
    spark.udf.registerJavaFunction("analyzerUdf", "com.amazon.productads.latac.udfs.Solr8LuceneAnalyzer")


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")

    working_folder = f"{s3_working_folder}{region}/{marketplace_name}/solr_index_{solr_index_latest_date}/"

    # S3 Output
    s3_head_asins = f"{working_folder}head_asins/"
    s3_not_matched_tcs = f"{working_folder}not_matched_tcs/"

    if s3_path_exists(sc, s3_not_matched_tcs):
        logger.info(f"{s3_not_matched_tcs} exists, return")
        return

    register_solr8_tokenizer()

    asin_expressions = get_janus_one_day_mt_data(args.marketplace_id, solr_index_latest_date, s3_head_asins)
    asin_normalized_tcs = get_asin_normalized_tcs(args.marketplace_id, asin_expressions)

    # Load solr_index
    s3_solr_index = f"s3://spear-shared/dataset/solr_index/ds={solr_index_latest_date}/region={region}/"
    solr_index = load_solr_index(args.marketplace_id, s3_solr_index)

    get_not_matched_tcs(asin_normalized_tcs, solr_index, s3_not_matched_tcs)


if __name__ == "__main__":
    main()


