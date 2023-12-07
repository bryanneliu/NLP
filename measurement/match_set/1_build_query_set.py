import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, lit, countDistinct, sum

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
    .appName("build_query_sets")
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

    parser.add_argument(
        "--daily_set_query_count",
        type=int,
        default=300000,
        help="Query count of the daily set",
        required=False
    )

    parser.add_argument(
        "--daily_tail_set_query_count",
        type=int,
        default=300000,
        help="Query count of the daily tail set",
        required=False
    )

    args, _ = parser.parse_known_args()

    return args

'''
https://searchdata.corp.amazon.com/searchdata-tommy-asin-schema
1-day Tommy query impression ASINs

Notes: 
1. Many queries with minimum ASIN position > 16
2. keywords, spelling_correction, sle_mlt_keywords
3. is_organic_result, is_sponsored_result
'''
def get_tommy_asins(args):
    # Date format to query tommy_asins (20220904)
    formatted_tommy_asins_date = format_date(args.match_set_on_date, "%Y%m%d")

    tommy_asins = spark.sql(f"""
        SELECT DISTINCT
            pseudo_session as session,
            pseudo_request_id as request_id,
            CASE
                WHEN sle_mlt_keywords is not NULL THEN sle_mlt_keywords
                WHEN is_auto_spell_corrected > 0 THEN spelling_correction 
                ELSE keywords
            END as raw_query,
            marketplace_id
        FROM searchdata.tommy_asin_v2_anonymized
        WHERE marketplace_id = {args.marketplace_id} 
            AND partition_date = '{formatted_tommy_asins_date}'
            AND is_spam_or_untrusted = 0
            AND is_suspect_session = 0
            AND search_type = 'kw'
            AND keywords is not NULL
            AND query_length >= 1
    """)

    print(f"Found tommy queries: {tommy_asins.count()}")
    return tommy_asins


def build_daily_query_set(args, tommy_asins, s3_raw_daily_query_set):
    daily_query_set = (tommy_asins
                        .select("session", "request_id", "marketplace_id", "raw_query")
                        .drop_duplicates()
                        .orderBy(rand())
                        .limit(args.daily_set_query_count)
                       )
    daily_query_set.repartition(10).write.mode("overwrite").parquet(s3_raw_daily_query_set)


def get_query_freq(tommy_asins):
    query_freq = tommy_asins.groupBy("raw_query", "marketplace_id").agg(
        countDistinct("request_id").alias("query_freq")
    ).cache()

    query_freq = query_freq.sort("query_freq", ascending=False).cache()

    total_query_count = query_freq.select(sum('query_freq')).collect()[0][0]
    logger.info(f"Total query count (with dupe): {total_query_count}")
    logger.info(f"Total unique query count (without dupe): {query_freq.count()}")

    # Query Statistics - calculate query freq for head/torso/tail/very_tail
    # head/torso/tail each covers 33% of traffic
    query_freq_list = [data[0] for data in query_freq.select("query_freq").collect()]

    query_freq_pre_sum = 0
    head_freq, torso_freq, top_90_freq = -1, -1, -1
    for i in range(1, len(query_freq_list) + 1):
        query_freq_pre_sum += query_freq_list[i - 1]

        if head_freq == -1 and query_freq_pre_sum > total_query_count / 3:
            head_freq = query_freq_list[i - 1]
            continue
        if torso_freq == -1 and query_freq_pre_sum > total_query_count / 3 * 2:
            torso_freq = query_freq_list[i - 1]
            continue
        if top_90_freq == -1 and query_freq_pre_sum > total_query_count * 0.9:
            top_90_freq = query_freq_list[i - 1]

    logger.info(f"Head query (top 33% of traffic) frequency: {head_freq}")
    logger.info(f"Torso query (middle 33% of traffic) frequency: {torso_freq}")
    logger.info(f"top 90% query frequency: {top_90_freq}")

    return query_freq, head_freq, torso_freq


def build_daily_tail_query_set(args, query_freq, s3_raw_daily_tail_query_set):
    daily_tail_query_set = query_freq.orderBy(rand()).limit(args.daily_tail_set_query_count)
    daily_tail_query_set.repartition(10).write.mode("overwrite").parquet(s3_raw_daily_tail_query_set)


def build_segment_query_set(query_freq, head_freq, torso_freq, s3_raw_segment_query_set):
    # Head
    head_query_set = query_freq.where(col("query_freq") >= head_freq).cache()
    head_query_set_count = head_query_set.count()
    head_query_set = head_query_set.withColumn("segment", lit("head"))

    # Torso
    torso_query_set = (query_freq
                        .where((col("query_freq") < head_freq) & (col("query_freq") >= torso_freq))
                        .orderBy(rand()).limit(head_query_set_count)
                        )
    torso_query_set = torso_query_set.withColumn("segment", lit("torso"))

    # Tail
    tail_query_set = (query_freq
                      .where(col("query_freq") < torso_freq).orderBy(rand())
                      .limit(2 * head_query_set_count)
                      )
    tail_query_set = tail_query_set.withColumn("segment", lit("tail"))

    logger.info(f"Head/Torso query set - unique query count: {head_query_set_count}")
    logger.info(f"Tail query set - unique query count: {2 * head_query_set_count}")

    # Merge
    segment_query_set = head_query_set.union(torso_query_set).union(tail_query_set)
    segment_query_set.repartition(10).write.mode("overwrite").parquet(s3_raw_segment_query_set)


'''
Build query sets from 1-day Tommy <query, all impression ASINs>
1. Daily set (randomly selected 500k requests)
2. Daily tail set (randomly selected 500k unique queries)
3. Segment set: 
   * Head covers the top 33% of traffic, torso covers the middle 33% of traffic, tail covers the bottom 33% of traffic
   * Head is to measure the precision and tail is to measure the recall
   * Head (all)
   * Torso (the same amount as Head)
   * Tail (3 * the amount of Head)
'''
def main():
    # Initialization
    args = process_args()

    marketplace = get_marketplace_name(args.marketplace_id).upper()

    # Match set date format (2022-12-11)
    formatted_match_set_date = format_date(args.match_set_on_date, "%Y-%m-%d")
    s3_raw_query_set = f"{s3_match_set_prefix}{marketplace}/{formatted_match_set_date}/raw_query_set/"

    # Query tommy_asins
    tommy_asins = get_tommy_asins(args)

    # Build daily query set
    if s3_path_exists(sc, s3_raw_query_set + "daily"):
        logger.info(f"{s3_raw_query_set} daily already exists, skip")
    else:
        build_daily_query_set(args, tommy_asins, s3_raw_query_set + "daily")

    # Build daily tail and segment query set
    if s3_path_exists(sc, s3_raw_query_set + "daily_tail") and s3_path_exists(sc, s3_raw_query_set + "segment"):
        logger.info(f"{s3_raw_query_set} daily_tail and segment already exist, skip")
    else:
        query_freq, head_freq, torso_freq = get_query_freq(tommy_asins)
        build_daily_tail_query_set(args, query_freq, s3_raw_query_set + "daily_tail")
        build_segment_query_set(query_freq, head_freq, torso_freq, s3_raw_query_set + "segment")


if __name__ == "__main__":
    main()
