import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, lit

from common import s3_working_folder
from marketplace_utils import get_region_name

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
    .appName("prepare_region_csv_data")
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

    args, _ = parser.parse_known_args()

    return args


def prepare_region_data(s3_input, s3_output, field="testDataset", is_normalized=False):
    logger.info("#####################################################################")
    logger.info(f"Prepare csv data for: {s3_input}")
    logger.info("#####################################################################")

    region = spark.read.parquet(s3_input)
    region = region.withColumnRenamed("tokens", field)  # "synonyms"
    if is_normalized:
        region = region.withColumn(field + "_isTextNormalized", lit("true"))  # "synonyms_isTextNormalized"

    region = region.coalesce(1)
    region.write.mode("overwrite").option("delimiter", "\t").option("header", True).csv(s3_output)

    markets = region.groupBy("marketplaceId").agg(count("asin").alias("asin_count"))
    markets.show(10, False)

    test = spark.read.csv(s3_output)
    region.printSchema()
    test.show(10, False)


def main():
    # Initialization
    args = process_args()

    region = get_region_name(args.marketplace_id).upper()
    working_folder = f"{s3_working_folder}{region}/"

    # Input
    s3_merged_top_expansion = f"{working_folder}*/merged_top_expansion/"

    # Output
    s3_region_csv = f"{working_folder}prod_csv/"

    overlap_query_threshold, overlap_tc_threshold, query_threshold = 0.5, 1, 0.3
    version = f"overlap_query_{str(overlap_query_threshold*100)}_tc_{str(overlap_tc_threshold*100)}_topQuery_{str(query_threshold*100)}/"
    prepare_region_data(f"{s3_merged_top_expansion}{version}", f"{s3_region_csv}{version}", field="testDataset", is_normalized=False)

    overlap_query_threshold, overlap_tc_threshold, query_threshold = 0.5, 1, -1
    version = f"overlap_query_{str(overlap_query_threshold*100)}_tc_{str(overlap_tc_threshold*100)}_topQuery_{str(query_threshold*100)}/"
    prepare_region_data(f"{s3_merged_top_expansion}{version}", f"{s3_region_csv}{version}", field="testDataset", is_normalized=False)

    overlap_query_threshold, overlap_tc_threshold, query_threshold = 0.5, 0.4, 0.25
    version = f"overlap_query_{str(overlap_query_threshold*100)}_tc_{str(overlap_tc_threshold*100)}_topQuery_{str(query_threshold*100)}/"
    prepare_region_data(f"{s3_merged_top_expansion}{version}", f"{s3_region_csv}{version}", field="testDataset", is_normalized=False)

    overlap_query_threshold, overlap_tc_threshold, query_threshold = 0.5, 0.4, -1
    version = f"overlap_query_{str(overlap_query_threshold*100)}_tc_{str(overlap_tc_threshold*100)}_topQuery_{str(query_threshold*100)}/"
    prepare_region_data(f"{s3_merged_top_expansion}{version}", f"{s3_region_csv}{version}", field="testDataset", is_normalized=False)


if __name__ == "__main__":
    main()

