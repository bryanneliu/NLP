import argparse
import logging
import sys

from pyspark.sql import SparkSession, Column
from pyspark.sql.column import _to_seq, _to_java_column
from pyspark.sql.functions import col, lit, udf, countDistinct, count, sum, min
from datetime import timedelta, datetime

from common import (
    s3_Tommy_top_impressions_prefix, s3_path_exists, get_query_token_count_UDF
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
    .appName("Tommy_get_top_impressions")
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
Get query set from tommy_asins
https://searchdata.corp.amazon.com/searchdata-tommy-asin-schema

Notes: 
1. Many queries with minimum ASIN position > 16
2. keywords, spelling_correction, sle_mlt_keywords
3. is_organic_result, is_sponsored_result
'''
def get_query_set(args, start_date_str, end_date_str, s3_raw_query):
    if s3_path_exists(sc, s3_raw_query):
        logger.info(f"{s3_raw_query} already exists, return")
        return

    requests = spark.sql(f"""
        SELECT DISTINCT
            pseudo_request_id as request_id,
            CASE
                WHEN sle_mlt_keywords is not NULL THEN sle_mlt_keywords
                WHEN is_auto_spell_corrected > 0 THEN spelling_correction 
                ELSE keywords
            END as raw_query
        FROM searchdata.tommy_asin_v2_anonymized
        WHERE marketplace_id = {args.marketplace_id} 
            AND partition_date >= '{start_date_str}'
            AND partition_date <= '{end_date_str}'
            AND is_spam_or_untrusted = 0
            AND is_suspect_session = 0
            AND search_type = 'kw'
            AND keywords is not NULL
            AND query_length >= 1
    """)

    raw_query_set = requests.groupBy("raw_query").agg(
        countDistinct("request_id").alias("query_freq")
    )

    logger.info(f"Total unique query count: {raw_query_set.count()}")

    raw_query_set = raw_query_set.coalesce(500)
    raw_query_set.write.mode("overwrite").parquet(s3_raw_query)

    logger.info(f"{s3_raw_query} is done writing to S3")


def build_match_set(args, date_range_start, date_range_end, s3_raw_query_set, s3_match_set):
    if s3_path_exists(sc, s3_match_set):
        logger.info(f"{s3_match_set} already exists, return")
        return

    raw_query_set = spark.read.parquet(s3_raw_query_set)

    tommy_asins = spark.sql(f"""
    SELECT
        CASE 
            WHEN sle_mlt_keywords is not NULL THEN sle_mlt_keywords
            WHEN is_auto_spell_corrected > 0 THEN spelling_correction 
            ELSE keywords
        END as raw_query,
        asin,
        position,
        is_sponsored_result,
        num_clicks,
        num_adds,
        num_consumes,
        num_purchases
    FROM searchdata.tommy_asin_v2_anonymized
    WHERE marketplace_id = {args.marketplace_id} 
        AND partition_date >= '{date_range_start}'
        AND partition_date <= '{date_range_end}'
        AND is_spam_or_untrusted = 0
        AND is_suspect_session = 0
        AND search_type = 'kw'
        AND keywords is not NULL
        AND query_length >= 1
        AND position < 100
    """)

    # Aggregate on <raw_query, ASIN> level
    tommy_asins = tommy_asins.groupBy("raw_query", "asin").agg(
        sum("is_sponsored_result").alias("sp_impressions"),
        count("*").alias("impressions"),
        min("position").alias("pos"),
        sum("num_clicks").alias("clicks"),
        sum("num_adds").alias("adds"),
        sum("num_purchases").alias("purchases"),
        sum("num_consumes").alias("consumes")
    )

    match_set = tommy_asins.join(raw_query_set, ["raw_query"], "inner")
    logger.info(f"match set - <raw_query, ASIN> pair count: {match_set.count()}")

    match_set = match_set.coalesce(500)
    match_set.write.mode("overwrite").parquet(s3_match_set)
    logger.info(f"Done with writing the match set to S3")


'''
Get Tommy_asin <query, top 100 impression ASINs> - Search and SP combined results
1. Get all unique queries: <raw_query, request_count(query_freq)>
2. Get all Tommy_asins: <raw_query, asin, user_engagement> join raw_query_set
   
Pyspark learnings:
1. coalesce: make the write unit smaller without reshuffle, repartition will reshuffle
2. Write the intermediate results to S3. Read them from S3 when using. Do not write to s3 and return dataframe simultaneously. 
'''
def main():
    # Initialization
    args = process_args()

    if args.task_i != 2:
        return

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    s3_root_folder = f"{s3_Tommy_top_impressions_prefix}{marketplace_name}/raw"

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    delta_end = timedelta(days=args.days_in_a_folder-1)
    delta = timedelta(days=args.days_in_a_folder)

    while start_date <= end_date:
        date_range_start = start_date.strftime("%Y%m%d")
        date_range_end = (start_date + delta_end).strftime("%Y%m%d")

        logger.info(f"{date_range_start} to {date_range_end} is starting.")

        s3_working_folder = f"{s3_root_folder}/{date_range_start}_{date_range_end}"

        s3_raw_query_set = f"{s3_working_folder}/raw_query/"
        s3_match_set = f"{s3_working_folder}/match_set/"

        get_query_set(args, date_range_start, date_range_end, s3_raw_query_set)
        build_match_set(args, date_range_start, date_range_end, s3_raw_query_set, s3_match_set)

        logger.info(f"{date_range_start} to {date_range_end} is done.")

        start_date += delta


if __name__ == "__main__":
    main()
