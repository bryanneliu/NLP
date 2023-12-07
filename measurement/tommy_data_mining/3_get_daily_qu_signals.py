import argparse
import logging
import sys

from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col, count, max
from datetime import timedelta, datetime

from common import (
    s3_Tommy_top_impressions_prefix, s3_path_exists
)
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
    .appName("Tommy_get_daily_qu_signals")
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
        help="Specify marketplaceid",
        required=True
    )

    parser.add_argument(
        "--start_date",
        type=str,
        default="2022-10-21",
        help="Start date",
        required=True
    )

    parser.add_argument(
        "--end_date",
        type=str,
        default="2022-12-24",
        help="End date",
        required=True
    )

    parser.add_argument(
        "--days_in_a_folder",
        type=int,
        default=5,
        help="Put 5 days data in a folder",
        required=True
    )

    parser.add_argument(
        "--task_i",
        type=int,
        default=1,
        help="Specify which task to run",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args

'''
Get QU signals from searchdata.tommy_searches_qu:
https://searchdata.corp.amazon.com/searchdata-tommy-searches-qu-schema
https://w.amazon.com/bin/view/Search/QueryUnderstanding/Metis/
https://w.amazon.com/bin/view/Search/QueryUnderstanding/QUSignals/Q2Specificity
'''
def get_daily_qu_signals(args, date_str, s3_daily_qu_signals):
    if s3_path_exists(sc, s3_daily_qu_signals):
        logger.info(f"{s3_daily_qu_signals} already exists, return")
        return

    qu = spark.sql(f"""
        SELECT
            DISTINCT
            request_id,
            CASE
                WHEN sle_mlt_keywords is not NULL THEN sle_mlt_keywords
                WHEN is_auto_spell_corrected > 0 THEN spelling_correction 
                ELSE keywords
            END as raw_query,
            metis,
            pt_scores,
            brand,
            gender,
            size,
            product_line,
            color,
            material,
            title,
            artist,
            query_specificity_click_entropy,
            query_specificity_purchase_entropy,
            partition_date
        FROM searchdata.tommy_searches_qu
        WHERE marketplace_id == {args.marketplace_id} 
            AND partition_date >= '{date_str}'
            AND partition_date <= '{date_str}'
            AND is_spam_or_untrusted = 0
            AND is_suspect_session = 0
            AND search_type = 'kw'
            AND keywords is not NULL
            AND query_length >= 1
    """)

    query = qu.groupBy("raw_query", "partition_date").agg(
        count("request_id").alias("query_freq"),
        max("metis").alias("metis"),
        max("pt_scores").alias("pt_scores"),
        max("brand").alias("brand"),
        max("gender").alias("gender"),
        max("size").alias("size"),
        max("product_line").alias("product_line"),
        max("color").alias("color"),
        max("material").alias("material"),
        max("title").alias("title"),
        max("artist").alias("artist"),
        max("query_specificity_click_entropy").alias("query_specificity_click_entropy"),
        max("query_specificity_purchase_entropy").alias("query_specificity_purchase_entropy")
    )

    non_tail_query = (
        query.where(col("query_freq") >= 10)
    )

    logger.info(f"unique query count: {query.count()}")
    logger.info(f"unique non_tail query count: {non_tail_query.count()}")
    
    non_tail_query = non_tail_query.coalesce(100)
    non_tail_query.write.mode("overwrite").parquet(s3_daily_qu_signals)


'''
Merge Daily QU signals to Monthly and Yearly QU signals:
1. Cluster by raw_query: Sum(query_freq)
2. Cluster by raw_query, sort by partition_date: Keep the top 1 (the most recent) metis and pt_scores
'''

def main():
    # Initialization
    args = process_args()

    if args.task_i != 3:
        return

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    s3_root_folder = f"{s3_Tommy_top_impressions_prefix}{marketplace_name}/qu/daily"

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    # This is a daily job, days_in_a_folder = 1
    delta = timedelta(days=args.days_in_a_folder)

    while start_date <= end_date:
        date_str = start_date.strftime("%Y%m%d")

        logger.info(f"{date_str} is starting.")

        s3_daily_qu_signals = f"{s3_root_folder}/{date_str}"

        get_daily_qu_signals(args, date_str, s3_daily_qu_signals)

        logger.info(f"{date_str} is done.")

        start_date += delta


if __name__ == "__main__":
    main()
