import argparse
import logging
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, sum, count, countDistinct, avg, array_contains, udf, explode, when, lit, max

from datetime import timedelta, datetime

import common as cm
from marketplace_utils import (
    get_marketplace_name,
    get_region_name
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
    .appName("weblab_join_full_janus")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext


def get_one_day_Janus_full_snapshot(args, date_str, region, s3_janus):
    if cm.s3_path_exists(sc, s3_janus):
        janus = spark.read.parquet(s3_janus)
        logger.info(f"{s3_janus} already exists, read Janus directly")
    else:
        date_folder = cm.format_date(date_str, "%Y/%m/%d")
        janus = spark.read.json(cm.Janus_snapshot_S3_prefix[region] + f"{date_folder}/*/update*.gz")

        janus = janus.where(col("marketplaceid") == args.marketplace_id)

        janus = janus.select("asin", "averageRating", "reviewCount", "adid", "adGroupId",
                             "hasTargetingExpression", "isForwardSearchable", explode("clause"),
                             "adGroupNegativeKeywordListId", "negativeKeywordListId")

        janus = janus.select("asin", "averageRating", "reviewCount", "adid", "adGroupId", "hasTargetingExpression",
                             "isForwardSearchable", "col.bid", "col.expression", "col.targetingClauseId", "col.type",
                             "adGroupNegativeKeywordListId", "negativeKeywordListId")

        # De-dupe as rating & review count can cause duplicates
        janus = janus.groupBy("asin", "adid", "adGroupId", "hasTargetingExpression",
                             "isForwardSearchable", "bid", "expression", "targetingClauseId", "type",
                             "adGroupNegativeKeywordListId", "negativeKeywordListId").agg(
            max("averageRating").alias("averageRating"),
            max("reviewCount").alias("reviewCount")
        )

        janus = janus.coalesce(5000)
        janus.write.mode("overwrite").parquet(s3_janus)
        logger.info(f"Done with writing Daily Janus to {s3_janus}")

    return janus


def get_normalized_Janus(janus):
    janus_updated = janus.withColumn("ATV2", when(col("hasTargetingExpression") == True, 1).otherwise(0))
    janus_updated = janus_updated.withColumn("MT", when(col("isForwardSearchable").isNull(), 1).otherwise(0))
    janus_updated = janus_updated.withColumn(
        "has_nkw",
        when(((col("adGroupNegativeKeywordListId").isNotNull()) |
            (col("negativeKeywordListId").isNotNull())), 1).otherwise(0)
    )
    janus_updated = janus_updated.withColumn("keyword_match_type",
                                             when(col("ATV2") == 1, col("expression")).otherwise(col("type")))
    janus_updated = janus_updated.withColumn("tc_expression_token_count", when(col("ATV2") == 1, 0).otherwise(
        cm.get_token_count_UDF(col("expression"))))
    janus_updated = janus_updated.drop("hasTargetingExpression", "isForwardSearchable")

    return janus_updated


def get_one_day_weblab_data(args, date_str):
    weblab = spark.sql(f"""
        SELECT *
        FROM {args.feed_glue_table}
        WHERE weblab_name = '{args.weblab_name}'
            AND feed_date == '{date_str}'
            AND marketplace_id = {args.marketplace_id}
    """)

    weblab = weblab.withColumnRenamed("targeting_clause_id", "targetingClauseId").withColumnRenamed("ad_id", "adid")
    weblab = weblab.withColumn("ad_type", cm.get_ad_type_UDF(col("ad_types_string"), lit(args.ad_type)))
    weblab = weblab.withColumn("query_token_count", cm.get_token_count_UDF(col("search_query")))
    weblab = weblab.withColumn('ops', F.aggregate("sale_price", F.lit(0.0), lambda acc, x: acc + x))

    weblab = weblab.select("feed_date", "treatment", "adid", "asin", "search_query", "imp_position", "cpc", "click",
                           "revenue", "ECTR", "ECVR", "page_number", "widget_name", "ad_types_string", "targetingClauseId",
                           "p_irrel", "campaign_id", "advertiser_id", "http_request_id", "unadjusted_bid", "session_id",
                           "ad_type", "query_token_count", "ops", "page_type")
    return weblab


'''
Get weblab full info:
1. Generate Janus full snapshot
2. Join Janus with weblab
'''
def main():
    # Initialization
    args = cm.process_args()

    region = get_region_name(args.marketplace_id).upper()
    marketplace = get_marketplace_name(args.marketplace_id).upper()

    s3_output = f"{cm.s3_weblab_analysis}{args.Solr_or_Horus.lower()}/{args.weblab_name}/{marketplace}/daily/"

    current_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    last_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    while current_date <= last_date:
        date_str = str(current_date)
        s3_janus = f"{cm.s3_weblab_analysis}Janus/{marketplace}/{date_str}"
        s3_searchWeblabJoinJanus = s3_output + date_str + "/searchWeblabJoinJanus/"

        if not cm.s3_path_exists(sc, s3_searchWeblabJoinJanus):
            janus = get_one_day_Janus_full_snapshot(args, date_str, region, s3_janus)
            janus_updated = get_normalized_Janus(janus)
            weblab = get_one_day_weblab_data(args, date_str)
            weblabJoinJanus = weblab.join(janus_updated, ["asin", "adid", "targetingClauseId"], "left")

            searchweblabJoinJanus = weblabJoinJanus.where(
                ~((col("ad_type") == "Other") & (col("keyword_match_type").isNull())))

            logger.info(f"search weblab join Janus query-ad count: {searchweblabJoinJanus.count()}")

            searchweblabJoinJanus = searchweblabJoinJanus.coalesce(5000)
            searchweblabJoinJanus.write.mode("overwrite").parquet(s3_searchWeblabJoinJanus)
            logger.info(f"Writing to {s3_searchWeblabJoinJanus} finished")

        current_date += timedelta(days=1)


if __name__ == "__main__":
    main()
