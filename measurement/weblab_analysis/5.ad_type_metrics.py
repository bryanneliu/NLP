import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round, when, sum, lit, coalesce, split, explode
import pyspark.sql.functions as F
from pyspark.sql.window import Window


import matplotlib.pyplot as plt
import io
import boto3

from datetime import timedelta, datetime

import common as cm
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
    .appName("ad_type_metrics")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext

def aggregate_by_columns(df, col_names):
    df = df.groupBy(*col_names).agg(
            F.count('*').alias('sum_impr'),
            round(sum("revenue"), 4).alias("sum_revenue"),
            round(count("asin"), 4).alias("asin_count"),
            round(sum("click"), 4).alias("click_count"),
            round(avg("click"), 4).alias("avg_ctr"),
            round(avg("p_irrel"), 4).alias("avg_p_irrel"),
            round(avg("ECTR"), 4).alias("avg_ectr"),
            round(avg("ECVR"), 4).alias("avg_ecvr"),
            round(avg("cpc"), 4).alias("avg_cpc"),
            round(sum("ops"), 4).alias("sum_ops"),
            round(sum("ops")*1.0/sum("revenue"), 4).alias("avg_roas"),
            round(F.count('*')*1.0/F.countDistinct("http_request_id"), 4).alias("ad_density"),
            F.countDistinct('asin', 'advertiser_id').alias('asin_advid_count')
        )
    return df

def add_shares_columns(df):
    df = (
        df.withColumn('rev_share', F.col('sum_revenue')/F.sum('sum_revenue').over(Window.partitionBy("treatment")))
        .withColumn('impr_share', F.col('sum_impr')/F.sum('sum_impr').over(Window.partitionBy("treatment")))
        .withColumn('avg_rev_per_impr', F.col('sum_revenue')/F.col('sum_impr'))
    )
    return df

def get_adtype_distribution(args, data):
    distribution = add_shares_columns(aggregate_by_columns(data, ["treatment", "ad_type"]))
    logger.info("#######################")
    logger.info("ad_type_distribution")
    logger.info("#######################")
    logger.info("Sort by sum_revenue desc")
    distribution.sort("sum_revenue", ascending=False).show(200, False)

    adtype_treatment = distribution.filter(col("ad_type") == (args.ad_type.lower()+"Only")).select('treatment').collect()[0][0]
    sum_impr = distribution.filter(f"treatment = '{adtype_treatment}'").select(sum('sum_impr')).collect()[0][0]
    adtype_impr = distribution.where(
            (col("ad_type") == (args.ad_type.lower()+"Only")) |
            (col("ad_type") == (args.ad_type.lower() + "Overlap"))
        ).select(sum('sum_impr')).collect()[0][0]
    adtype_only_impr = distribution.where(
            (col("ad_type") == (args.ad_type.lower()+"Only"))
        ).select(sum('sum_impr')).collect()[0][0]    
    logger.info(f"Adtype total sourced impressions share over all impression is {adtype_impr}/{sum_impr} = {adtype_impr/sum_impr}")
    logger.info(f"Adtype uniquely sourced impressions share over all impression is {adtype_only_impr}/{sum_impr} = {adtype_only_impr/sum_impr}")
    logger.info(f"Adtype uniquely sourced impressions ratio over this adtype total is {adtype_only_impr}/{adtype_impr} = {adtype_only_impr/adtype_impr}")



def get_keyword_match_type_distribution(data):
    distribution = add_shares_columns(aggregate_by_columns(data, ["treatment", "keyword_match_type"]))
    logger.info("keyword_match_type_distribution:")
    distribution.sort("keyword_match_type", "sum_revenue", ascending=False).show(200, False)

    distribution = add_shares_columns(aggregate_by_columns(data, ["treatment", "keyword_match_type", "has_nkw"]))
    logger.info("keyword_match_type_with_nkw_distribution:")
    distribution.sort("keyword_match_type", "has_nkw", "sum_revenue", ascending=False).show(200, False)


def get_query_token_count_distribution(data):
    
    AT = add_shares_columns(aggregate_by_columns(data.where(col("ATV2") == 1), ["treatment", "query_token_count"]))
    logger.info("AT - query token count distribution:")
    AT.sort("sum_revenue", ascending=False).show(50, False)

    MT = add_shares_columns(
        aggregate_by_columns(data.where(col("ATV2") == 0), ["treatment", "query_token_count"])
            .where(col("query_token_count") < 11).sort("query_token_count", "click_count", ascending=False)
        )
    logger.info("MT - query token count distribution:")
    MT.sort("sum_revenue", ascending=False).show(50, False)


def get_widget_distribution(data):
    distribution = add_shares_columns(aggregate_by_columns(data, ["treatment", "widget_name"]))
    logger.info("widget_distribution:")
    distribution.sort("sum_revenue", ascending=False).show(200, False)

    distribution = add_shares_columns(aggregate_by_columns(data.filter('page_number = 1'), ["treatment", "widget_name"]))
    logger.info("widget_distribution (page 1 only):")
    distribution.sort("sum_revenue", ascending=False).show(200, False)


def get_adtype_coverage(data):
    # Show data
    pos1_all = data.where((col("page_number") == 1) & (col("imp_position") == 1))
    pos1_total = (pos1_all
                  .groupBy("treatment")
                  .agg(count("asin").alias("treatment_asin_count"))
                  )

    pos1_adtype_coverage = (
        pos1_all
        .groupBy("treatment", "ad_type")
        .agg(count("asin").alias("ad_type_asin_count"))
        .join(pos1_total, ["treatment"])
        .withColumn("percent", round(col("ad_type_asin_count") / col("treatment_asin_count"), 6) * 100)
        .select("treatment", "ad_type", "percent")
    )

    logger.info("Pos1 Ads ad_type distribution:")
    pos1_adtype_coverage.sort("treatment").show(200, False)

    # Show fig
    top4_all = data.where((col("page_number") == 1) & (col("imp_position") <= 4))
    top4_total = (top4_all
                  .groupBy("feed_date", "treatment")
                  .agg(count("asin").alias("treatment_asin_count"))
                  )

    top4_adtype_coverage = (top4_all
                            .groupBy("feed_date", "treatment", "ad_type")
                            .agg(count("asin").alias("ad_type_asin_count"))
                            .join(top4_total, ["feed_date", "treatment"])
                            .withColumn("percent", round(col("ad_type_asin_count") / col("treatment_asin_count"), 6) * 100)
                            .select("feed_date", "treatment", "ad_type", "percent")
                        )

    return top4_adtype_coverage


def fig_to_s3(fig, bucket, path):
    s3 = boto3.resource('s3')
    img_data = io.BytesIO()
    fig.savefig(img_data, format='png', bbox_inches='tight')
    img_data.seek(0)
    object = s3.Object(bucket, path)

    # Upload the image to s3
    object.put(Body=img_data.read())


def generate_adtype_coverage_report(args, marketplace, top4_adtype_coverage):
    s3_key_prefix = f"weblab_analysis/solr/{args.weblab_name}/{marketplace}/ad_type/"

    fig, ax = plt.subplots(3, 1, sharex=True, constrained_layout=True, figsize=(5, 10))

    df = top4_adtype_coverage.toPandas()

    # searchOnly -> SolrOnly, other -> Horus, searchOverlap -> Overlap
    for ad_type, ax in zip(["searchOnly", "Other", "searchOverlap"], ax.reshape(-1)):
        for treatment, color in zip(["T1", "T2", "T3"], ["red", "blue", "green"]):
            df[(df["ad_type"] == ad_type) & (df["treatment"] == treatment)].plot(kind="scatter", x="feed_date",
                                                                                 y="percent", label=treatment,
                                                                                 color=color, s=32, ax=ax)
        ax.set_title(ad_type)
        ax.tick_params(axis='x', labelrotation=45)

    fig_to_s3(fig, cm.s3_bucket, f"{s3_key_prefix}adType_coverage.png")


def compare_ad_type_revenue_ctr(data):
    # Compare for individual ad_type
    expanded_data = data.withColumn("individual_ad_type", explode(split(col("ad_types_string"), ',')))

    statistics = aggregate_by_columns(expanded_data, ["individual_ad_type"])
    logger.info("########################################################################")
    logger.info("Individual ad_type comparison - source by ad_type (only & overlapped)")
    logger.info("########################################################################")
    logger.info("Rank by sum_revenue desc")
    statistics.sort("sum_revenue", ascending=False).show(100, False)
    logger.info("Rank by avg_ctr desc")
    statistics.sort("avg_ctr", ascending=False).show(100, False)

    statistics = aggregate_by_columns(data.where(~col("ad_types_string").contains(",")), ["ad_types_string"])
    logger.info("########################################################################")
    logger.info("Individual ad_type comparison - source by ad_type only")
    logger.info("########################################################################")
    logger.info("Rank by sum_revenue desc")
    statistics.sort("sum_revenue", ascending=False).show(100, False)
    logger.info("Rank by avg_ctr desc")
    statistics.sort("avg_ctr", ascending=False).show(100, False)


'''
1. Calculate ad_type coverage in weblab impression
    a. requests with ad_type returned
    b. ad_depth
2. Keyword match type distribution in requests with ad_type returned
3. Keyword match type distribution in requests with ad_type clicked
4. Query count distribution in requests with ad_type returned
5. Query count distribution in requests with ad_type clicked
6. Query samples with ad_type on pos_1: clicked and non-clicked
   <query, asin, keyword_match_type, expression>
'''
def main():
    # Initialization
    args = cm.process_args()

    marketplace = get_marketplace_name(args.marketplace_id).upper()

    data = cm.load_impressed_ad_details(spark, args)

    get_adtype_distribution(args, data)
    compare_ad_type_revenue_ctr(data)

    if args.Solr_or_Horus.lower() == "solr":
        logger.info("#############################")
        logger.info("Solr-based ad_type analysis")
        logger.info("#############################")
        top4_adtype_coverage = get_adtype_coverage(data)
        generate_adtype_coverage_report(args, marketplace, top4_adtype_coverage)


    logger.info("#######################")
    logger.info("data_with_ad_type (Only and Overlap)")
    logger.info("#######################")
    data_with_ad_type = (
        data.where(
            (col("ad_type") == (args.ad_type.lower()+"Only")) |
            (col("ad_type") == (args.ad_type.lower() + "Overlap"))
        )
    )
    get_keyword_match_type_distribution(data_with_ad_type)
    get_query_token_count_distribution(data_with_ad_type)
    get_widget_distribution(data_with_ad_type)

    logger.info("#######################")
    logger.info("data_with_ad_type (Only)")
    logger.info("#######################")
    data_with_ad_type_only = (
        data.where(
            (col("ad_type") == (args.ad_type.lower()+"Only"))
        )
    )
    get_keyword_match_type_distribution(data_with_ad_type_only)
    get_query_token_count_distribution(data_with_ad_type_only)
    get_widget_distribution(data_with_ad_type_only)

    logger.info("#######################")
    logger.info("widget_distribution on Control")
    logger.info("#######################")
    control = "T1" if args.Solr_or_Horus.lower() == "solr" else "C"
    data_C = (
        data.where(
            (col("treatment") == (control))
        )
    )
    get_widget_distribution(data_C)

    logger.info("#####################################")
    logger.info("Bad AT (p_irrel > 0.2) and MT (p_irrel > 0.15) in data_with_ad_type")
    logger.info("#####################################")
    AT = data_with_ad_type.where(col("ATV2") == 1)
    bad_AT = AT.where(col("p_irrel") > 0.2)
    AT_count, bad_AT_count = AT.count(), bad_AT.count()

    MT = data_with_ad_type.where(col("ATV2") == 0)
    bad_MT = MT.where(col("p_irrel") > 0.15)
    MT_count, bad_MT_count = MT.count(), bad_MT.count()

    bad_AT_ratio, bad_MT_ratio = None, None
    if AT_count > 0:
        bad_AT_ratio = bad_AT_count/AT_count
    if MT_count > 0:
        bad_MT_ratio = bad_MT_count/MT_count
    logger.info(f"AT total count: {AT_count}, bad AT total count: {bad_AT_count}, the ratio is {bad_AT_ratio}")
    logger.info(f"MT total count: {MT_count}, bad MT total count: {bad_MT_count}, the ratio is {bad_MT_ratio}\n\n")


    logger.info("################################################")
    logger.info("Bad AT (p_irrel > 0.2) and MT (p_irrel > 0.15) in clicked_data_with_ad_type")
    logger.info("################################################")

    clicked_data_with_ad_type = data_with_ad_type.where(col("click") > 0)

    clicked_AT = clicked_data_with_ad_type.where(col("ATV2") == 1)
    bad_clicked_AT = clicked_AT.where(col("p_irrel") > 0.2)
    clicked_AT_count, bad_clicked_AT_count = clicked_AT.count(), bad_clicked_AT.count()

    clicked_MT = clicked_data_with_ad_type.where(col("ATV2") == 0)
    bad_clicked_MT = clicked_MT.where(col("p_irrel") > 0.15)
    clicked_MT_count, bad_clicked_MT_count = clicked_MT.count(), bad_clicked_MT.count()

    bad_ATClick_ratio, bad_MTClick_ratio = None, None
    if clicked_AT_count > 0:
        bad_ATClick_ratio = bad_clicked_AT_count/clicked_AT_count
    if clicked_MT_count > 0:
        bad_MTClick_ratio = bad_clicked_MT_count/clicked_MT_count
    logger.info(f"AT total count: {clicked_AT_count}, bad AT total count: {bad_clicked_AT_count}, the ratio is {bad_ATClick_ratio}")
    logger.info(f"MT total count: {clicked_MT_count}, bad MT total count: {bad_clicked_MT_count}, the ratio is {bad_MTClick_ratio}\n\n")


    logger.info("###########################################")
    logger.info(f"Samples sourced by {args.ad_type}")
    logger.info("###########################################")

    logger.info("Bad AT samples:")
    bad_AT.select("search_query", "asin", "keyword_match_type", "p_irrel", "cpc").show(200, False)

    logger.info("Bad MT samples:")
    bad_MT.select("search_query", "asin", "keyword_match_type", "expression", "p_irrel", "cpc").show(200, False)

    logger.info("TOP (by impression position, <5) engaged (clicked) samples on Page 1:")
    sample_clicked = (
        clicked_data_with_ad_type
            .where(col("page_number") == 1)
            .where(col("imp_position") < 5)
    )
    sample_clicked.select("search_query", "asin", "keyword_match_type", "expression", "p_irrel", "cpc").show(200, False)

    logger.info("TOP (by impression position, <5) not engaged (clicked) samples on Page 1:")
    sample_not_clicked = (
        data_with_ad_type
            .where(col("click") == 0)
            .where(col("page_number") == 1)
            .where(col("imp_position") < 5)
    )
    sample_not_clicked.select("search_query", "asin", "keyword_match_type", "expression", "p_irrel", "cpc").show(200, False)


if __name__ == "__main__":
    main()