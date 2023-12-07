import argparse
import logging
import sys

from pyspark.sql import SparkSession
# from pyspark.sql import Column
# from pyspark.sql.column import _to_seq, _to_java_column
from pyspark.sql.functions import col, count, lit
# from pyspark.sql.window import Window

from utils import *
from expansion_udf import (
    pt_to_add_UDF
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
    .appName("Product_type_based_expansion")
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

'''
def solr5_tokenize(text, marketplace_id):
    sc = SparkSession.builder.getOrCreate()
    _solr5_tp = sc._jvm.com.amazon.productads.latac.udfs.Solr5AnalyzerPySparkUDF.solr5TokenizeUdf()
    return Column(_solr5_tp.apply(_to_seq(sc, [text, marketplace_id], _to_java_column)))
'''


def load_nota_asin_attributes(args):
    nota_asin_attributes = spark.sql(f"""
        SELECT DISTINCT asin, lower(replace(product_type, '_', ' ')) AS product_type
        FROM default.nota_asin_attributes
        WHERE marketplaceid = {args.marketplace_id}
            AND product_type is not null
    """)

    '''
    asin_normalized_product_type = (
        nota_asin_attributes
            .withColumn("normalized_product_type", solr5_tokenize(col("product_type"), lit(args.marketplace_id).cast('long')))
            .drop("product_type")
    )
    '''

    nota_asin_attributes.show(10, False)
    return nota_asin_attributes

def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")
    s3_working_folder = f"{s3_sp_not_matched_set_prefix}{marketplace_name}/solr_index_{solr_index_latest_date}/"
    s3_product_type_based_expansion = f"{s3_working_folder}product_type_based_expansion/"

    if s3_path_exists(sc, s3_product_type_based_expansion):
        logger.info(f"{s3_product_type_based_expansion} exists, return")
        return

    logger.info("Load missing tokens:")
    missing_tokens = load_missing_tokens(s3_working_folder)

    logger.info("Load ASIN product type:")
    nota_asin_attributes = load_nota_asin_attributes(args)

    logger.info("Join <asin, expansion> with <asin, product_type>:")
    asin_expansion_pt = (
        missing_tokens.join(nota_asin_attributes, ["asin"], "inner")
    )
    asin_expansion_pt.show(5, False)

    logger.info("Product type as an expansion:")
    asin_expansion = (
        asin_expansion_pt
            .withColumn("pt_to_add", pt_to_add_UDF(col("expansion"), col("product_type")))
            .where(col("pt_to_add") == 1)
            .select("asin", "expansion")
            .drop_duplicates()
    )
    logger.info(f"asin_expansion count: {asin_expansion.count()}")

    logger.info(f"Start to write asin_expansion to S3:")
    asin_expansion = asin_expansion.coalesce(100)
    asin_expansion.write.mode("overwrite").parquet(s3_product_type_based_expansion)


if __name__ == "__main__":
    main()
