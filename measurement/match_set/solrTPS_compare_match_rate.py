import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, regexp_replace, when, min, max, sum

from common import (
    format_date, s3_path_exists,
    s3_solr_index_prefix, s3_match_set_prefix_for_solrTPS,
    is_matched_UDF
)

from marketplace_utils import (
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
    .appName("compare_solrTPS_match_rate")
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

    parser.add_argument(
        "--s3_solrTPS",
        type=str,
        default="s3://tps-diff-output/TPSGenCompareDiffOutput/llo-na-m1/6059177c-ed34-4390-9597-ff7cf7d91b20/",
        help="S3 location of SolrTPS results",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


def load_solrTPS_results(args):
    solrTPS = spark.read.json(args.s3_solrTPS)
    logger.info("SolrTPS Samples:")
    solrTPS.show(5, False)
    request_count = solrTPS.select("requestId").drop_duplicates().count()
    logger.info(f"SolrTPS request count: {request_count}")
    return solrTPS


def get_query_set(solrTPS, marketplace_id):
    query_set = solrTPS.where(col("marketplaceId") == marketplace_id).select("solrQueryString").drop_duplicates()
    query_set = query_set.withColumn("raw_query", regexp_replace('solrQueryString', '\\+', ' '))
    logger.info(f"Unique queries in SolrTPS: {query_set.count()}")
    logger.info("Query set Samples:")
    query_set.show(5, False)
    return query_set


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


def build_match_set(query_set, request_level_tommy_asins, solr_index, solr_index_asin, s3_match_set):
    match_set = query_set.join(request_level_tommy_asins, ["raw_query"], "inner")

    # Aggregate on query-asin level
    # Assume the recall is same, but the ranking is different, merge all ASINs and keep an ASIN once for each query
    match_set = (
        match_set
        .groupBy("raw_query", "asin")
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

    sp_match_set = (
        match_set
        .join(solr_index_asin.hint("shuffle_hash"), ["asin"], "inner")
        .join(solr_index.hint("shuffle_hash"), ["asin"], "inner")
        .withColumn("is_title_matched", is_matched_UDF(col("raw_query"), col("title_tokens")))
        .withColumn("is_index_matched", is_matched_UDF(col("raw_query"), col("index_tokens")))
    )

    logger.info(f"SolrTPS match set query-asin count: {sp_match_set.count()}")

    query_count = sp_match_set.select("raw_query").drop_duplicates().count()
    logger.info(f"Unique queries in match set: {query_count}")

    sp_match_set = sp_match_set.coalesce(1000)
    sp_match_set.write.mode("overwrite").parquet(s3_match_set)


def compare_asin_match_rates(dataset):
    qa_count_baseline = dataset.where(col("baseline").isNotNull()).count()
    qa_count_candidate = dataset.where(col("candidate").isNotNull()).count()
    qa_count = dataset.count()

    logger.info(f"ASIN level index match rate - baseline: {round(qa_count_baseline/qa_count, 4)}")
    logger.info(f"ASIN level index match rate - candidate: {round(qa_count_candidate/qa_count, 4)}\n")

def compare_ad_match_rates(dataset):
    qa_count_baseline = dataset.where(col("baseline").isNotNull()).select(sum(col("baseline_ad_count"))).collect()[0][0]
    qa_count_candidate = dataset.where(col("candidate").isNotNull()).select(sum(col("candidate_ad_count"))).collect()[0][0]
    qa_count = dataset.count()

    logger.info(f"Ad level index match rate - baseline: {round(qa_count_baseline/qa_count, 4)}")
    logger.info(f"Ad level index match rate - candidate: {round(qa_count_candidate/qa_count, 4)}\n")

def compare_match_rates_all(args, s3_match_set):
    solrTPS = spark.read.json(args.s3_solrTPS)
    keptSolrTPS = solrTPS.where((col("adId") != -1) & (col("tcId") != -1))
    keptSolrTPS = keptSolrTPS.withColumnRenamed("solrQueryString", "raw_query")

    baseline = keptSolrTPS.where(col("isBaseline") == True).select("raw_query", "asin", lit("baseline"))
    baseline = baseline.groupBy("raw_query", "asin", "baseline").agg(count("*").alias("baseline_ad_count"))

    candidate = keptSolrTPS.where(col("isBaseline") == False).select("raw_query", "asin", lit("candidate"))
    candidate = candidate.groupBy("raw_query", "asin", "candidate").agg(count("*").alias("candidate_ad_count"))

    match_set = spark.read.parquet(s3_match_set)
    logger.info("Match set Full Schema:")
    match_set.printSchema()

    match_set = match_set.withColumn("is_engaged", when(col("clicks") > 0, 1).otherwise(0))
    match_set = match_set.select("raw_query", "asin", "min_pos", "is_engaged")

    match_set_combined = match_set.join(baseline, ["raw_query", "asin"], "left")
    match_set_combined = match_set_combined.join(candidate, ["raw_query", "asin"], "left")

    logger.info("Match set Samples:")
    match_set_combined.show(5, False)

    logger.info("Overall:")
    compare_asin_match_rates(match_set_combined)
    compare_ad_match_rates(match_set_combined)

    logger.info("Engaged:")
    engaged_match_set_combined = match_set_combined.where(col("is_engaged") == 1)
    compare_asin_match_rates(engaged_match_set_combined)
    compare_ad_match_rates(engaged_match_set_combined)

    logger.info("Top 5 ASINs:")
    top_match_set_combined = match_set_combined.where(col("min_pos") <= 5)
    compare_asin_match_rates(top_match_set_combined)
    compare_ad_match_rates(top_match_set_combined)

    logger.info("First page ASINs:")
    first_page_match_set_combined = match_set_combined.where(col("min_pos") <= 16)
    compare_asin_match_rates(first_page_match_set_combined)
    compare_ad_match_rates(first_page_match_set_combined)


def main():
    # Initialization
    args = process_args()

    region = get_region_name(args.marketplace_id).upper()

    if args.s3_solrTPS.lower() == "none":
        return

    if not s3_path_exists(sc, args.s3_solrTPS):
        logger.info(f"{args.s3_solrTPS} does not exists, return")
        return

    solrTPS = load_solrTPS_results(args)
    query_set = get_query_set(solrTPS, args.marketplace_id)
    request_level_tommy_asins = get_tommy_asins(args)
    solr_index, solr_index_asin = load_solr_index(args, region, args.marketplace_id)

    output_folder = [part for part in args.s3_solrTPS.split("/") if part != ""][-1]
    s3_match_set = s3_match_set_prefix_for_solrTPS + output_folder

    build_match_set(query_set, request_level_tommy_asins, solr_index, solr_index_asin, s3_match_set)
    compare_match_rates_all(args, s3_match_set)


if __name__ == "__main__":
    main()
