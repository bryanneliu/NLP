import argparse
import logging
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, sum, min, max, count, avg
from collections import OrderedDict
from common import *

from marketplace_utils import (
    get_marketplace_name,
    get_region_realm,
    get_janus_bucket,
    JANUS_PROJECTED_SCHEMA,
)
from s3_utils import JanusSnapShotReader


logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
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

N_HORUS_QASIN = "nHorusQAsin"
TRAFFIC_COVERAGE_COL = "TrafficCoverage"
QUERY_SEG_COVERAGE_COL = "QueryCoverage"
TOTAL_OVERLAP_HORUS_COL = "TotalOverlapQA/HorusQA"
NotInSP_OVERLAP_HORUS_COL = "TotalOverlapQAExcludeSP/HorusQA"
TOTAL_OVERLAP_IMPR_COL = "TotalOverlapQA/TommyQA"
NotInSP_OVERLAP_IMPR_COL = "TotalOverlapQAExcludeSP/TommyQA"
ASIN_DEPTH_COL = "AverageAsinDepthPerQuery"
TOMMY24W_OVERLAP_HORUS_COL = "OverlapWithTommy24W/HorusQA"


def process_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--marketplace_id", type=int, default=6, help="Specify marketplaceid", required=True
    )

    parser.add_argument(
        "--match_set_on_date",
        type=str,
        default="20221212",
        help="The date should be in this format and be used in tommy_asin",
        required=True,
    )

    parser.add_argument(
        "--rawQA_or_cacheQA",
        type=str,
        default="rawQA",
        choices=["rawQA", "cacheQA"],
        help="Can take both raw QA set (assuming parquet and has columns query and asin), and Horus cache dataset in json format",
        required=True,
    )

    parser.add_argument(
        "--horus_data_input_s3_path",
        type=str,
        default=None,
        help="s3 path for input Horus dataset",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return args


def load_horus_QA(args):
    horus_tcid_df = None
    if args.rawQA_or_cacheQA == "rawQA":
        horus_qa_df = spark.read.parquet(args.horus_data_input_s3_path)
        colnames = horus_qa_df.columns
        if not (("query" in colnames) and ("asin" in colnames)):
            raise Exception(
                f"Input Horus dataset {args.horus_data_input_s3_path} does not have columns named query and asin!"
            )
        horus_qa_df = horus_qa_df.select("query", "asin").distinct()
    else:
        horus_qa_df = spark.read.json(args.horus_data_input_s3_path)
        horus_tcid_df = (
            horus_qa_df.withColumn("ad_exp", F.explode("ad"))
            .withColumn("asin", F.col("ad_exp.asin"))
            .withColumn("targetingClauseId", F.col("ad_exp.targetingClauseId"))
            .withColumn("adId", F.col("ad_exp.adId"))
            .withColumnRenamed("searchQuery", "query")
            .select("query", "asin", "targetingClauseId", "adId")
            .distinct()
        )
        horus_qa_df = horus_tcid_df.select("query", "asin").distinct()

    return horus_qa_df, horus_tcid_df


def get_most_recent_snapshot_from_bucket(
    bucket: str, date: str, lookback_hours=8, region="USAmazon"
) -> str:
    """
    Get the largest Janus snapshot within lookback_hours window. Date has to be in "YYYY/MM/DD" or %Y/%m/%d format
    """
    janus_reader = JanusSnapShotReader(
        bucket, date, region=region, lookback_hours=lookback_hours, anchor_now=False
    )
    janus_path = janus_reader.get_largest_snapshot_in_lookback_window()
    logger.info(f"Best Janus snapshot path is {janus_path}")
    return janus_path


def read_JanusS3_as_table(s3_path) -> str:
    janus = spark.read.json(s3_path, schema=JANUS_PROJECTED_SCHEMA)
    logger.info("Read JanusData Done")
    return janus


def get_tommy_asins(args):
    """
    https://searchdata.corp.amazon.com/searchdata-tommy-asin-schema
    1-day Tommy query impression ASINs

    Notes:
    1. Many queries with minimum ASIN position > 16
    2. keywords, spelling_correction, sle_mlt_keywords
    3. is_organic_result, is_sponsored_result
    """
    # Date format to query tommy_asins (20220904)
    formatted_tommy_asins_date = format_date(args.match_set_on_date, "%Y%m%d")

    tommy_asins = spark.sql(
        f"""
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
    """
    )

    # Aggregate on request level: An ASIN can appear multiple times in a search (from SP and organic search both)
    request_level_tommy_asins = tommy_asins.groupBy(
        "pseudo_session", "pseudo_request_id", "raw_query", "asin"
    ).agg(
        max("is_sponsored_result").alias("is_sponsored_result"),
        count("*").alias("query_asin_impressions"),
        min("position").alias("min_pos"),
        sum("num_clicks").alias("clicks"),
        sum("num_adds").alias("adds"),
        sum("num_purchases").alias("purchases"),
        sum("num_consumes").alias("consumes"),
    )

    return request_level_tommy_asins


def horus_qa_overlap_analysis(daily_match_set, horus_qa_df, metrics_dict, prefix=""):
    n_horus_qa = horus_qa_df.count()
    metrics_dict[N_HORUS_QASIN] = n_horus_qa
    daily_match_set_qa = daily_match_set.select("query", "asin").distinct()
    n_impr_qa = daily_match_set_qa.count()

    n_intersect = horus_qa_df.join(daily_match_set_qa, ["query", "asin"], "left_semi").count()
    metrics_dict[f"{prefix}{TOTAL_OVERLAP_HORUS_COL}"] = round(n_intersect / n_horus_qa, 5)
    metrics_dict[f"{prefix}{TOTAL_OVERLAP_IMPR_COL}"] = round(n_intersect / n_impr_qa, 5)

    daily_match_set_qa_org = (
        daily_match_set.filter(F.col("is_sponsored_result") <= "0")
        .select("query", "asin")
        .distinct()
    )
    n_intersect = horus_qa_df.join(daily_match_set_qa_org, ["query", "asin"], "left_semi").count()
    metrics_dict[f"{prefix}{NotInSP_OVERLAP_HORUS_COL}"] = round(n_intersect / n_horus_qa, 5)
    metrics_dict[f"{prefix}{NotInSP_OVERLAP_IMPR_COL}"] = round(n_intersect / n_impr_qa, 5)

    return metrics_dict


def analyze_daily_match_set(
    metrics_dict, daily_query_set, request_level_tommy_asin, horus_qa_df, asin_attr
):
    n_sampled_traffic = daily_query_set.count()
    n_sampled_traffic_intersect_horus = daily_query_set.join(
        horus_qa_df, daily_query_set.raw_query == horus_qa_df.query, "left_semi"
    ).count()
    metrics_dict[TRAFFIC_COVERAGE_COL] = round(
        n_sampled_traffic_intersect_horus / n_sampled_traffic, 5
    )

    daily_match_set = daily_query_set.join(
        request_level_tommy_asin, ["session", "request_id", "raw_query"], "inner"
    ).withColumnRenamed("raw_query", "query")
    horus_qa_overlap_analysis(daily_match_set, horus_qa_df, metrics_dict)
    asin_depth = (
        horus_qa_df.groupBy("query")
        .agg(F.count("*").alias("n_asin"))
        .select(F.mean("n_asin"))
        .collect()[0][0]
    )
    metrics_dict[f"{ASIN_DEPTH_COL}"] = round(asin_depth, 5)

    daily_match_set_qa = daily_match_set.select("query", "asin").distinct()
    horus_qa_df_additional = horus_qa_df.join(
        daily_query_set, daily_query_set.raw_query == horus_qa_df.query, "left_semi"
    ).join(daily_match_set_qa, ["query", "asin"], "left_anti")
    logger.info(
        f"Horus QA set additionally sourced asins within sampled query set: {horus_qa_df_additional.count()} QA pairs, samples below:"
    )
    horus_qa_df_additional.join(asin_attr.select("asin", "item_name"), "asin").show(100, False)


def analyze_segment_match_set(
    metrics_dict, segment_query_set, request_level_tommy_asin, horus_qa_df
):

    segment_match_set = segment_query_set.join(request_level_tommy_asin, ["raw_query"], "inner")

    segment_match_set = (
        segment_match_set.groupBy("raw_query", "segment", "asin")
        .agg(
            max("is_sponsored_result").alias("is_sponsored_result"),
            sum("query_asin_impressions").alias("query_asin_impressions"),
            min("min_pos").alias("min_pos"),
            sum("clicks").alias("clicks"),
            sum("adds").alias("adds"),
            sum("purchases").alias("purchases"),
            sum("consumes").alias("consumes"),
        )
        .withColumnRenamed("raw_query", "query")
    )

    for seg in ["head", "torso", "tail"]:
        n_segment_traffic = (
            segment_query_set.filter(f"segment = '{seg}'")
            .select(F.sum("query_freq"))
            .collect()[0][0]
        )
        n_covered = (
            segment_query_set.withColumnRenamed("raw_query", "query")
            .filter(f"segment = '{seg}'")
            .join(horus_qa_df, "query", "left_semi")
            .select(F.sum("query_freq"))
            .collect()[0][0]
        )
        metrics_dict[f"{seg}_{QUERY_SEG_COVERAGE_COL}"] = round(n_covered / n_segment_traffic, 5)
        horus_qa_overlap_analysis(
            segment_match_set.filter(f"segment = '{seg}'"),
            horus_qa_df,
            metrics_dict,
            prefix=f"{seg}_",
        )
        asin_depth = (
            horus_qa_df.join(
                segment_query_set.withColumnRenamed("raw_query", "query").filter(
                    f"segment = '{seg}'"
                ),
                "query",
                "left_semi",
            )
            .groupBy("query")
            .agg(F.count("*").alias("n_asin"))
            .select(F.mean("n_asin"))
            .collect()[0][0]
        )
        metrics_dict[f"{seg}_{ASIN_DEPTH_COL}"] = round(asin_depth, 5)


def match_type_distribution(args, janus_bucket, region_realm, horus_tcid_df):
    if args.rawQA_or_cacheQA != "cacheQA":
        return
    logger.info(f"Input data is cacheQA, do match type distribution check")
    logger.info("======== Match type distribution analysis =======")
    s3_path = get_most_recent_snapshot_from_bucket(
        janus_bucket, format_date(args.match_set_on_date, "%Y/%m/%d"), region=region_realm
    )
    janus = read_JanusS3_as_table(s3_path)
    janus_exp = (
        janus.withColumn("clause_info", F.explode(F.col("clause")))
        .withColumn("targetingClauseId", F.col("clause_info").targetingClauseId)
        .withColumn("bid", F.col("clause_info").bid)
        .withColumn(
            "type",
            F.when(
                F.col("clause_info").type.isin("broad", "exact", "phrase"),
                F.col("clause_info").type,
            ).otherwise(F.col("clause_info").expression),
        )
        .drop(*["clause", "clause_info"])
    )
    horus_tcid_janus_df = horus_tcid_df.join(janus_exp.drop("asin"), ["adId", "targetingClauseId"])
    horus_tcid_janus_df_bytype = (
        horus_tcid_janus_df.groupBy("type")
        .agg(
            F.count("*").alias("n"),
            F.mean("bid").alias("avg_bid"),
            F.countDistinct("query", "asin").alias("n_distinct_query_asin"),
            F.countDistinct("asin").alias("n_distinct_asin"),
            F.countDistinct("targetingClauseId").alias("n_distinct_tcid"),
            F.countDistinct("adId").alias("n_distinct_adId"),
        )
        .orderBy(F.col("n").desc())
    )
    horus_tcid_janus_df_bytype.show(100, False)


def main():
    """
    Build SP match sets for Horus:
    1. Join tommy_asins with raw_query_set
    2. Join with given Horus QA set, assuming schema has query, asin columns
    3. Calculate Horus QA set overlap with organic, with SP, with organic-SP, and other
    """

    # Initialization
    args = process_args()
    logger.info(f"Start, arguments are {args}")

    marketplace = get_marketplace_name(args.marketplace_id).upper()
    janus_bucket = get_janus_bucket(args.marketplace_id)
    region_realm = get_region_realm(args.marketplace_id)

    # Match set date format (2022-12-11)
    formatted_match_set_date = format_date(args.match_set_on_date, "%Y-%m-%d")
    s3_match_set_folder = f"{s3_match_set_prefix}{marketplace}/{formatted_match_set_date}/"

    # Load query set
    s3_raw_query_set = s3_match_set_folder + "raw_query_set/"
    raw_daily_query_set = spark.read.parquet(s3_raw_query_set + "daily")
    raw_segment_query_set = spark.read.parquet(s3_raw_query_set + "segment")

    # Load asin attributes
    PREFIX = "s3://nota-offline-pipeline/prod/"
    marketplace = marketplace.lower()
    asin_attr_path = f"{PREFIX}/modelFactory/asinAttributes/v1/parquet/{marketplace}/release"
    asin_attr = spark.read.parquet(asin_attr_path)

    # Load Horus QA set
    horus_qa_df, horus_tcid_df = load_horus_QA(args)

    # Get request-level tommy asins
    request_level_tommy_asins = get_tommy_asins(args)

    # Coverage analysis
    metrics_dict = OrderedDict()
    analyze_daily_match_set(
        metrics_dict, raw_daily_query_set, request_level_tommy_asins, horus_qa_df, asin_attr
    )
    analyze_segment_match_set(
        metrics_dict, raw_segment_query_set, request_level_tommy_asins, horus_qa_df
    )

    # Tommy Asin Aggregates Coverage Analysis, source data https://w.amazon.com/bin/view/SSPA/SB/Nota/ProductDetails/#HTommyAsinAggregated24wv3
    PREFIX = "s3://nota-offline-pipeline/prod/"
    marketplace = "us"
    tommyasin_path = f"{PREFIX}/tommyAsin/queryAsinAgg24w/v3/parquet/{marketplace}/release/"
    tommyasin_agg = spark.read.parquet(tommyasin_path)

    n_horus_qa = horus_qa_df.count()
    n_intersect = horus_qa_df.join(tommyasin_agg, ["query", "asin"], "left_semi").count()
    metrics_dict[TOMMY24W_OVERLAP_HORUS_COL] = round(n_intersect / n_horus_qa, 5)

    logger.info("======== Overall Coverage Analysis =======")
    summary = spark.createDataFrame(
        data=[list(metrics_dict.values())], schema=list(metrics_dict.keys())
    ).toPandas()
    logger.info(summary.T)

    horus_qa_df_additional = horus_qa_df.join(tommyasin_agg, ["query", "asin"], "left_anti")
    logger.info(
        f"Horus QA set additionally sourced asins beyond 24W TommyAsinAgg: {horus_qa_df_additional.count()} QA pairs, samples below:"
    )
    horus_qa_df_additional.join(asin_attr.select("asin", "item_name"), "asin").show(100, False)

    # check Janus match type distribution only for CacheQA
    match_type_distribution(args, janus_bucket, region_realm, horus_tcid_df)

    logger.info(f"All done")


if __name__ == "__main__":
    main()
