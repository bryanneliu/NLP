import argparse
import logging
import sys

from pyspark.sql import SparkSession, Column
from pyspark.sql.column import _to_seq, _to_java_column
from pyspark.sql.functions import col, lit, udf, explode, collect_set, array, concat_ws

from expansion_udf import (
    expand_expressions_for_synonym_dict
)

from utils import (
    format_date, get_s3_sp_not_matched_set_prefix, s3_path_exists,
    load_jp_synonym, load_reviewed_synonym
)

from marketplace_utils import (
    get_marketplace_name, get_janus_bucket, get_region_realm, get_region_name
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
    .appName("expression_based_synonym_expansion")
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
        "--solr_index_latest_ds",
        type=str,
        default="2022-12-19",
        help="Specify solr_index ds",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


def solr5_tokenize(text, marketplace_id):
    sc = SparkSession.builder.getOrCreate()
    _solr5_tp = sc._jvm.com.amazon.productads.latac.udfs.Solr5AnalyzerPySparkUDF.solr5TokenizeUdf()
    return Column(_solr5_tp.apply(_to_seq(sc, [text, marketplace_id], _to_java_column)))


def get_Janus_mt_one_day_snapshot(args, solr_index_asin):
    janus_date_folder = format_date(args.solr_index_latest_ds, "%Y/%m/%d")

    janus = spark.read.json(f"s3://{get_janus_bucket(args.marketplace_id)}/{get_region_realm(args.marketplace_id)}/{janus_date_folder}/*/update*.gz")

    janus_mt = (
        janus
            .where(col("marketplaceid") == args.marketplace_id)
            .where(col("isForwardSearchable").isNull())  # MT
            .select("asin", "marketplaceid", explode("clause"))
            .select("asin", "marketplaceid", "col.expression")
    )
    logger.info("janus_mt is done")

    asin_group = (
        janus_mt
            .groupBy("asin", "marketplaceid")
            .agg(
                collect_set("expression").alias("expression_set")
            )
            .withColumn(
                "expression_str",
                concat_ws(" ", col("expression_set"))
            )
            .select("asin", "marketplaceid", "expression_str")
    )
    logger.info("Get ASIN merged expression str")
    asin_group.show(5, True)

    asin_group = asin_group.join(solr_index_asin, ["asin"], "inner")
    logger.info(f"Target ASIN count: {asin_group.count()}")

    return asin_group


def normalize_asin_expression(asin_group, s3_janus_mt_normalized):
    asin_group_normalized = (
        asin_group
            .withColumn("normalized_expression_str", solr5_tokenize(col("expression_str"), col("marketplaceid")))
            .select("asin", "normalized_expression_str")
    )
    logger.info("Tokenization is done")

    asin_group_normalized = asin_group_normalized.coalesce(5000)
    asin_group_normalized.write.mode("overwrite").parquet(s3_janus_mt_normalized)
    logger.info(f"Done with writing Daily Normalized Janus MT to {s3_janus_mt_normalized}")

    return asin_group_normalized


def expression_based_synonym_expansion(asin_group_normalized, solr_index, synonym_dict, s3_expression_synonym_expansion):
    asin_group_with_index = asin_group_normalized.join(solr_index, ["asin"], "inner")

    expansion_udf = expand_expressions_for_synonym_dict(synonym_dict)

    expanded_asin = (
        asin_group_with_index
            .withColumn(
                "expansions",
                expansion_udf(
                    col("normalized_expression_str"),
                    col("title_tokens"),
                    col("index_tokens")
                )
            )
            .where(col("expansions") != array())
            .select("asin", explode(col("expansions")))
            .select("asin", "col.expansion", "col.debug")
    )
    logger.info("Done with synonym expansion on Janus MT expressions.")
    expanded_asin.show(5, False)

    expanded_asin = expanded_asin.coalesce(5000)
    expanded_asin.write.mode("overwrite").parquet(s3_expression_synonym_expansion)
    logger.info(f"Done writing ASIN expression expansion to S3.")


def load_solr_index(args, solr_index_latest_date):
    region = get_region_name(args.marketplace_id).upper()

    s3_solr_index = f"s3://spear-shared/dataset/solr_index/ds={solr_index_latest_date}/region={region}/"
    solr_index = spark.read.parquet(s3_solr_index)
    solr_index = solr_index.where(col("marketplaceid") == args.marketplace_id).select("asin", "title_tokens", "index_tokens")
    solr_index_asin = solr_index.select("asin")
    solr_index_asin.cache()
    logger.info(f"solr_index unique ASIN count: {solr_index_asin.count()}")
    return solr_index_asin, solr_index


'''
1. Tokenize Janus MT expressions
2. Synonym expansion on MT expressions
'''
def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()

    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")
    s3_sp_not_matched_set_prefix = get_s3_sp_not_matched_set_prefix(region)

    s3_working_folder = f"{s3_sp_not_matched_set_prefix}{marketplace_name}/solr_index_{solr_index_latest_date}/"
    s3_janus_mt_normalized = f"{s3_working_folder}janus_mt_normalized/"
    s3_expression_synonym_expansion = f"{s3_working_folder}reviewed_synonym_expansion_for_expressions/"

    if s3_path_exists(sc, s3_expression_synonym_expansion):
        logger.info(f"{s3_expression_synonym_expansion} already exists, the job is done")
        return

    solr_index_asin, solr_index = load_solr_index(args, solr_index_latest_date)

    if s3_path_exists(sc, s3_janus_mt_normalized):
        logger.info(f"{s3_janus_mt_normalized} already exists, read directly")
        asin_group_normalized = spark.read.parquet(s3_janus_mt_normalized)
    else:
        asin_group = get_Janus_mt_one_day_snapshot(args, solr_index_asin)
        asin_group_normalized = normalize_asin_expression(asin_group, s3_janus_mt_normalized)

    if args.marketplace_id == 6:
        synonym_dict = load_jp_synonym(spark, logger)
    else:
        synonym_dict = load_reviewed_synonym(spark, logger)

    expression_based_synonym_expansion(asin_group_normalized, solr_index, synonym_dict,
                                       s3_expression_synonym_expansion)


if __name__ == "__main__":
    main()