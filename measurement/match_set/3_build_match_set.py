import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, min, max, count, avg

from common import *

from marketplace_utils import (
    get_marketplace_name,
    get_region_name
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
    .appName("build_match_sets")
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
        "--latest_solr_index_date",
        type=str,
        default="2022-12-09",
        help="The date should be in this format and be used in tommy_asin",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


def load_solr_index(args, region, marketplace):
    # Date format to query solr_index (2022-12-11)
    formatted_solr_index_date = format_date(args.latest_solr_index_date, "%Y-%m-%d")
    s3_solr_index = f"{s3_solr_index_prefix}{formatted_solr_index_date}/region={region}/"

    solr_index = spark.read.parquet(s3_solr_index)

    solr_index = (solr_index
                  .where(col("marketplaceid") == args.marketplace_id)
                  .select("asin", "title_tokens", "index_tokens")
                  )

    solr_index_asin = solr_index.select("asin").cache()
    logger.info(f"solr_index unique ASIN count in marketplace {marketplace}: {solr_index_asin.count()}")

    return solr_index, solr_index_asin


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
            marketplace_id,
            asin,
            position,
            is_sponsored_result,
            num_clicks,
            num_adds,
            num_consumes,
            num_purchases
        FROM searchdata.tommy_asin_v2_anonymized
        WHERE marketplace_id == {args.marketplace_id} 
            AND partition_date = '{formatted_tommy_asins_date}'
            AND is_spam_or_untrusted = 0
            AND is_suspect_session = 0
            AND search_type = 'kw'
            AND keywords is not NULL
            AND query_length >= 1
    """)

    # Aggregate on request level: An ASIN can appear multiple times in a search (from SP and organic search both)
    request_level_tommy_asins = tommy_asins.groupBy("session", "request_id", "raw_query", "asin").agg(
        max("is_sponsored_result").alias("is_sponsored_result"),
        count("*").alias("query_asin_impressions"),
        min("position").alias("min_pos"),
        sum("num_clicks").alias("clicks"),
        sum("num_adds").alias("adds"),
        sum("num_purchases").alias("purchases"),
        sum("num_consumes").alias("consumes")
    )

    return request_level_tommy_asins


def build_daily_match_set(daily_query_set, request_level_tommy_asin, solr_index, solr_index_asin, s3_daily_match_set):
    if s3_path_exists(sc, s3_daily_match_set):
        logger.info(f"{s3_daily_match_set} already exists, return")
        return

    daily_match_set = daily_query_set.join(request_level_tommy_asin, ["session", "request_id", "raw_query"], "inner")

    sp_daily_match_set = (
        daily_match_set
            .join(solr_index_asin.hint("shuffle_hash"), ["asin"], "inner")
            .join(solr_index.hint("shuffle_hash"), ["asin"], "inner")
            .withColumn("is_title_matched", is_matched_UDF(col("normalized_query"), col("title_tokens")))
            .withColumn("is_index_matched", is_matched_UDF(col("normalized_query"), col("index_tokens")))
            .withColumn("is_title_bmm_matched", is_matched_with_bmm_UDF(col("normalized_query"), col("title_tokens")))
            .withColumn("is_index_bmm_matched", is_matched_with_bmm_UDF(col("normalized_query"), col("index_tokens")))
        )

    logger.info(f"SP daily match set query-asin count: {sp_daily_match_set.count()}")

    sp_daily_match_set = sp_daily_match_set.coalesce(1000)
    sp_daily_match_set.write.mode("overwrite").parquet(s3_daily_match_set)


def build_daily_tail_match_set(daily_tail_query_set, request_level_tommy_asin, solr_index, solr_index_asin, s3_daily_tail_match_set):
    if s3_path_exists(sc, s3_daily_tail_match_set):
        logger.info(f"{s3_daily_tail_match_set} already exists, return")
        return

    daily_tail_match_set = daily_tail_query_set.join(request_level_tommy_asin, ["raw_query"], "inner")

    # Aggregate on query-asin level
    # Assume the recall is same, but the ranking is different, merge all ASINs and keep an ASIN once for each query
    daily_tail_match_set = (
        daily_tail_match_set
            .groupBy("raw_query", "normalized_query", "query_freq", "query_token_count", "asin")
            .agg(
                sum("is_sponsored_result").alias("is_sponsored_result_count"),
                sum("query_asin_impressions").alias("query_asin_impressions"),
                min("min_pos").alias("min_pos"),
                sum("clicks").alias("clicks"),
                sum("adds").alias("adds"),
                sum("purchases").alias("purchases"),
                sum("consumes").alias("consumes")
            )
        )

    sp_daily_tail_match_set = (
        daily_tail_match_set
            .join(solr_index_asin.hint("shuffle_hash"), ["asin"], "inner")
            .join(solr_index.hint("shuffle_hash"), ["asin"], "inner")
            .withColumn("is_title_matched", is_matched_UDF(col("normalized_query"), col("title_tokens")))
            .withColumn("is_index_matched", is_matched_UDF(col("normalized_query"), col("index_tokens")))
            .withColumn("is_title_bmm_matched", is_matched_with_bmm_UDF(col("normalized_query"), col("title_tokens")))
            .withColumn("is_index_bmm_matched", is_matched_with_bmm_UDF(col("normalized_query"), col("index_tokens")))
    )

    logger.info(f"SP daily tail match set query-asin count: {sp_daily_tail_match_set.count()}")

    sp_daily_tail_match_set = sp_daily_tail_match_set.coalesce(1000)
    sp_daily_tail_match_set.write.mode("overwrite").parquet(s3_daily_tail_match_set)


def build_segment_match_set(segment_query_set, request_level_tommy_asin, solr_index, solr_index_asin, s3_segment_match_set):
    if s3_path_exists(sc, s3_segment_match_set):
        logger.info(f"{s3_segment_match_set} already exists, return")
        return

    segment_match_set = segment_query_set.join(request_level_tommy_asin, ["raw_query"], "inner")

    segment_match_set = (
        segment_match_set
            .groupBy("raw_query", "normalized_query", "query_freq", "query_token_count", "segment", "asin")
            .agg(
                sum("is_sponsored_result").alias("is_sponsored_result_count"),
                sum("query_asin_impressions").alias("query_asin_impressions"),
                min("min_pos").alias("min_pos"),
                sum("clicks").alias("clicks"),
                sum("adds").alias("adds"),
                sum("purchases").alias("purchases"),
                sum("consumes").alias("consumes")
            )
        )

    sp_segment_match_set = (
        segment_match_set
            .join(solr_index_asin.hint("shuffle_hash"), ["asin"], "inner")
            .join(solr_index.hint("shuffle_hash"), ["asin"], "inner")
            .withColumn("is_title_matched", is_matched_UDF(col("normalized_query"), col("title_tokens")))
            .withColumn("is_index_matched", is_matched_UDF(col("normalized_query"), col("index_tokens")))
            .withColumn("is_title_bmm_matched", is_matched_with_bmm_UDF(col("normalized_query"), col("title_tokens")))
            .withColumn("is_index_bmm_matched", is_matched_with_bmm_UDF(col("normalized_query"), col("index_tokens")))
    )

    logger.info(f"SP daily segment match set query-asin count: {sp_segment_match_set.count()}")

    sp_segment_match_set = sp_segment_match_set.coalesce(1000)
    sp_segment_match_set.write.mode("overwrite").parquet(s3_segment_match_set)


'''
Build SP match sets:
1. Join tommy_asins with normalized_query_set
2. Join match_set with SP solr_index
3. Calculate is_title_matched and is_index_matched using normalized_query and index_tokens full match
'''
def main():
    # Initialization
    args = process_args()

    region = get_region_name(args.marketplace_id).upper()
    marketplace = get_marketplace_name(args.marketplace_id).upper()

    # Match set date format (2022-12-11)
    formatted_match_set_date = format_date(args.match_set_on_date, "%Y-%m-%d")
    s3_match_set_folder = f"{s3_match_set_prefix}{marketplace}/{formatted_match_set_date}/"

    # Load query set
    s3_normalized_query_set = s3_match_set_folder + "normalized_query_set/"
    normalized_daily_query_set = spark.read.parquet(s3_normalized_query_set + "daily")
    normalized_daily_tail_query_set = spark.read.parquet(s3_normalized_query_set + "daily_tail")
    normalized_segment_query_set = spark.read.parquet(s3_normalized_query_set + "segment")

    # Load solr_index
    solr_index, solr_index_asin = load_solr_index(args, region, marketplace)

    # Get request-level tommy asins
    request_level_tommy_asins = get_tommy_asins(args)

    # Build match sets
    s3_match_set_folder = s3_match_set_folder + "match_set/"

    build_daily_match_set(
        normalized_daily_query_set,
        request_level_tommy_asins,
        solr_index,
        solr_index_asin,
        s3_match_set_folder + "daily"
    )

    build_daily_tail_match_set(
        normalized_daily_tail_query_set,
        request_level_tommy_asins,
        solr_index,
        solr_index_asin,
        s3_match_set_folder + "daily_tail"
    )

    build_segment_match_set(
        normalized_segment_query_set,
        request_level_tommy_asins,
        solr_index,
        solr_index_asin,
        s3_match_set_folder + "segment"
    )

    logger.info(f"Check match set in {s3_match_set_folder}")


if __name__ == "__main__":
    main()
