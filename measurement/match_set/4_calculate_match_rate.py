import argparse
import logging
import sys

from pyspark.sql import SparkSession, Column
from pyspark.sql.column import _to_seq, _to_java_column
from pyspark.sql.functions import col, sum, min, max, count, avg, when, lit, concat, expr
from pyspark.sql.types import LongType, StringType
import  pyspark.sql.functions as F
from common import *
import numpy as np

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
    .appName("calculate_match_rate_improvement")
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
        "--S3_dataset",
        type=str,
        default="s3://synonym-expansion-experiments/asin_level_synonym_expansion/title_dataset/uk/20221008/parquet/",
        help="Calculate match rate for the dataset - the dataset contains 3 columns (asin, marketplace_id, tokens) in parquet format",
        required=True
    )

    parser.add_argument(
        "--require_tokenization",
        type=int,
        default=0,
        help="If the dataset was not normalized, set require_tokenization to 1 to call solr 8 tokenizer for analyzing the data set",
        required=True
    )

    parser.add_argument(
        "--S3_dataset_format",
        type=str,
        default="parquet",
        choices=["parquet", "json"],
        help="input dataset format",
        required=False
    )

    parser.add_argument(
        "--join_with_matchset",
        type=str,
        default="left",
        choices=["left", "inner"],
        help="How to join with matchset",
        required=False
    )

    parser.add_argument(
        "--enable_added_token_stats",
        type=int,
        default=0,
        help="whether to enable added_token status",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


def calculate_index_match_rate(match_set):
    statistics = match_set.select(
        count("*").alias("all_query_asin"),
        sum(col('is_title_matched')).alias('title_matched_count'),
        sum(col('is_index_matched')).alias('index_matched_count')
    )

    statistics = statistics.select(
        "*",
        (col("title_matched_count") / col("all_query_asin")).alias("title_match_rate"),
        (col("index_matched_count") / col("all_query_asin")).alias("index_match_rate")
    )

    statistics.show(10, False)


def calculate_index_metrics(match_set):
    # Calculate overall index metrics
    logger.info("Overall - index match rate:")
    calculate_index_match_rate(match_set)

    # Calculate first page ASINs index metrics
    # first page ASIN: ASIN appeared in first page at lease once
    first_page_match_set = match_set.where(col("min_pos") <= 16)
    logger.info("First Page - index match rate:")
    calculate_index_match_rate(first_page_match_set)

    # Calculate top 5 ASINs index metrics
    top_5_match_set = match_set.where(col("min_pos") <= 5)
    logger.info("Top 5 - index match rate:")
    calculate_index_match_rate(top_5_match_set)

    # Calculate engaged ASINs index metrics
    engaged_match_set = (
        match_set.where((col("clicks") > 0) | (col("adds") > 0) | (col("purchases") > 0) | (col("consumes") > 0))
    )
    logger.info("Engaged - index match rate:")
    calculate_index_match_rate(engaged_match_set)


def calculate_request_metrics(match_set):
    request_groups = match_set.groupBy("session", "request_id").agg(
        count("*").alias("all_query_asin"),
        sum(col('is_title_matched')).alias('title_match_count'),
        sum(col('is_index_matched')).alias('index_match_count')
    )

    request_groups = (
        request_groups
            .withColumn("title_match_rate", col("title_match_count")/col("all_query_asin"))
            .withColumn("index_match_rate", col("index_match_count")/col("all_query_asin"))
    )

    request_groups.createOrReplaceTempView("request_groups")

    # Request AVG match rate
    request_match_rate = spark.sql(f"""
        SELECT "Sampled requests" as name,
               avg(title_match_rate) as avg_title_match_rate,
               avg(index_match_rate) as avg_index_match_rate
        FROM request_groups
    """)

    logger.info("Daily match set request-level metric:")
    request_match_rate.show(10, False)

    # Request percentage without match
    request_percent = spark.sql(f"""
        SELECT COUNT(*) AS request_count,
               COUNT_IF(index_match_rate == 0.0) AS request_without_match_count
        FROM request_groups
    """)

    request_percent = (
        request_percent
            .withColumn(
                "request_percent_without_match",
                col("request_without_match_count") / col("request_count")
            )
    )

    request_percent.show(10, False)


def calculate_query_match_rate_by_segment(joined_set):
    query_groups = joined_set.groupBy("raw_query", "segment").agg(
        count("asin").alias("asin_count"),
        sum("is_title_matched").alias("title_match_count"),
        sum("is_index_matched").alias("index_match_count")
    )

    query_groups = (
        query_groups
            .withColumn("title_match_rate", col("title_match_count") / col("asin_count"))
            .withColumn("index_match_rate", col("index_match_count") / col("asin_count"))
    )

    query_groups.createOrReplaceTempView("query_groups")

    query_match_rate = spark.sql(f"""
        SELECT "Overall query set" as name,
               avg(title_match_rate) as avg_title_match_rate,
               avg(index_match_rate) as avg_index_match_rate
        FROM query_groups
        UNION ALL
        SELECT "Head query set" as name,
               avg(title_match_rate) as avg_title_match_rate,
               avg(index_match_rate) as avg_index_match_rate
        FROM query_groups
        WHERE segment = 'head'
        UNION ALL
        SELECT "Torso query set" as name,
               avg(title_match_rate) as avg_title_match_rate,
               avg(index_match_rate) as avg_index_match_rate
        FROM query_groups
        WHERE segment = 'torso'
        UNION ALL
        SELECT "Tail query set" as name,
               avg(title_match_rate) as avg_title_match_rate,
               avg(index_match_rate) as avg_index_match_rate
        FROM query_groups
        WHERE segment = 'tail'
    """
    )

    query_match_rate.show(10, False)


def calculate_query_metrics(match_set):
    logger.info("Overall - query match rate by segment:")
    calculate_query_match_rate_by_segment(match_set)

    engaged_match_set = (
        match_set.where((col("clicks") > 0) | (col("adds") > 0) | (col("purchases") > 0) | (col("consumes") > 0))
    )
    logger.info("Engaged - query match rate by segment:")
    calculate_query_match_rate_by_segment(engaged_match_set)


def join_matchset_dataset(match_set, dataset, join):
    joined_set = match_set.join(dataset, ["asin"], join)

    joined_set = (
        joined_set
            .withColumn(
                "is_index_matched_with_expansion",
                is_matched_with_expansion_UDF(
                    col("normalized_query"),
                    col("index_tokens"),
                    col("tokens")
                )
            )
            .withColumn(
                "added_tokens",
                generate_extra_tokens_UDF(
                    col("index_tokens"),
                    col("tokens")
                )
            )
            .withColumn("n_added_tokens", F.size("added_tokens"))
    )

    logger.info("New matches with expansion:")
    samples = (
        joined_set
            .where(
                (col("is_index_matched") == 0) &
                (col("is_index_matched_with_expansion") == 1)
            )
            .select("normalized_query", "asin", "tokens", "added_tokens", "n_added_tokens")
    )

    samples.show(100, False)

    return joined_set


def calculate_index_match_rate_improved(joined_set):
    cnt_cond = lambda cond: sum(when(cond, 1).otherwise(0))

    statistics = joined_set.select(
        count("*").alias("all_query_asin"),
        cnt_cond(col('is_index_matched') == 1).alias('index_matched_count'),
        cnt_cond(col('is_index_matched_with_expansion') == 1).alias('index_matched_with_expansion_count')       
    )

    statistics = statistics.select(
        "*",
        (col('index_matched_count') / col('all_query_asin')).alias('index_match_rate'),
        (col('index_matched_with_expansion_count') / col('all_query_asin')).alias('index_match_rate_with_expansion')
    )

    statistics = (
        statistics
            .withColumn("improved", (col("index_match_rate_with_expansion") / col("index_match_rate")-1))
            .withColumn("improved_percent", concat((col("improved")*100).cast(StringType()), lit("%")))
    )

    metrics = statistics.select(
        "all_query_asin",
        "index_match_rate",
        "index_match_rate_with_expansion",
        "improved_percent"
    )

    metrics.show(10, False)

def calculate_added_token_stats(joined_set):
    cnt_cond = lambda cond: sum(when(cond, 1).otherwise(0))
    perc = list(np.arange(0, 1.1, 0.1))

    stats = joined_set.select(        
        count("*").alias("all_query_asin"),
        cnt_cond(col('n_added_tokens') > 0).alias('added_token_gt0_count'),
        F.mean("n_added_tokens").alias('avg_n_added_tokens')
    ).select(
        '*',
        (F.col("added_token_gt0_count")/F.col("all_query_asin")).alias('added_token_rate')
    )

    stats_amongadded = joined_set.filter('n_added_tokens > 0').select(        
        count("*").alias("all_query_asin"),
        F.mean("n_added_tokens").alias('avg_n_added_tokens'),
        F.percentile_approx("n_added_tokens", perc).alias("n_added_tokens_deciles"),
    )

    stats_asin_gt0_c = joined_set.filter('n_added_tokens > 0').select('asin').distinct().count()
    stats_asin_all_c = joined_set.select('asin').distinct().count()
    asin_gt0 = joined_set.filter('n_added_tokens > 0').select('asin', 'n_added_tokens').distinct()
    logger.info(f"==== Asin level: {stats_asin_gt0_c} / {stats_asin_all_c}"
                f" = {round(stats_asin_gt0_c/stats_asin_all_c, 4)} total asins has added tokens, "
                "n_added_tokens distribution among asins with added tokens:")
    logger.info(list(zip(perc, asin_gt0.approxQuantile("n_added_tokens", perc, 0.05))))    

    logger.info("==== All QA pairs:")
    stats.show(10, False)
    logger.info("==== Among QA pairs which has added tokens:")
    stats_amongadded.show(10, False)
    

def calculate_index_metrics_improved(args, joined_set):
    # Calculate overall index metrics
    logger.info("Overall - index match rate improved:")
    calculate_index_match_rate_improved(joined_set)

    if args.enable_added_token_stats:
        calculate_added_token_stats(joined_set)

    # Calculate first page ASINs index metrics
    # first page ASIN: ASIN appeared in first page at lease once
    first_page_joined_set = joined_set.where(col("min_pos") <= 16)
    logger.info("First Page - index match rate improved:")
    calculate_index_match_rate_improved(first_page_joined_set)

    if args.enable_added_token_stats:
        calculate_added_token_stats(first_page_joined_set)

    # Calculate top 5 ASINs index metrics
    top_5_joined_set = joined_set.where(col("min_pos") <= 5)
    logger.info("Top 5 - index match rate improved:")
    calculate_index_match_rate_improved(top_5_joined_set)
    if args.enable_added_token_stats:
        calculate_added_token_stats(top_5_joined_set)

    # Calculate engaged ASINs index metrics
    engaged_joined_set = (
        joined_set.where((col("clicks") > 0) | (col("adds") > 0) | (col("purchases") > 0) | (col("consumes") > 0))
    )
    logger.info("Engaged - index match rate improved:")
    calculate_index_match_rate_improved(engaged_joined_set)
    if args.enable_added_token_stats:
        calculate_added_token_stats(engaged_joined_set)


def calculate_query_match_rate_improved_by_segment(joined_set):
    query_groups = joined_set.groupBy("raw_query", "segment").agg(
        count("asin").alias("asin_count"),
        sum("is_index_matched").alias("index_match_count"),
        sum("is_index_matched_with_expansion").alias("index_match_count_with_expansion")
    )

    query_groups = (
        query_groups
            .withColumn("index_match_rate", col("index_match_count") / col("asin_count"))
            .withColumn("index_match_rate_with_expansion", col("index_match_count_with_expansion") / col("asin_count"))
    )

    query_groups.createOrReplaceTempView("query_groups")

    query_match_rate = spark.sql(f"""
        SELECT "Overall query set" as name,
               avg(index_match_rate) as avg_index_match_rate,
               avg(index_match_rate_with_expansion) as avg_index_match_rate_with_expansion
        FROM query_groups
        UNION ALL
        SELECT "Head query set" as name,
               avg(index_match_rate) as avg_index_match_rate,
               avg(index_match_rate_with_expansion) as avg_index_match_rate_with_expansion
        FROM query_groups
        WHERE segment = 'head'
        UNION ALL
        SELECT "Torso query set" as name,
               avg(index_match_rate) as avg_index_match_rate,
               avg(index_match_rate_with_expansion) as avg_index_match_rate_with_expansion
        FROM query_groups
        WHERE segment = 'torso'
        UNION ALL
        SELECT "Tail query set" as name,
               avg(index_match_rate) as avg_index_match_rate,
               avg(index_match_rate_with_expansion) as avg_index_match_rate_with_expansion
        FROM query_groups
        WHERE segment = 'tail'
    """
    )

    query_match_rate = (
        query_match_rate
            .withColumn("improved", (col("avg_index_match_rate_with_expansion") / col("avg_index_match_rate") - 1))
            .withColumn("improved_percent", concat((col("improved")*100).cast(StringType()), lit("%")))
    )

    query_match_rate.show(10, False)


def calculate_query_metrics_improved(joined_set):

    # Calculate overall query metrics for segment joined set
    logger.info("Overall - query match rate improved by segment:")
    calculate_query_match_rate_improved_by_segment(joined_set)

    engaged_joined_set = (
        joined_set.where((col("clicks") > 0) | (col("adds") > 0) | (col("purchases") > 0) | (col("consumes") > 0))
    )
    logger.info("Engaged - query match rate improved by segment:")
    calculate_query_match_rate_improved_by_segment(engaged_joined_set)


def solr5_tokenize(text, marketplace_id):
    sc = SparkSession.builder.getOrCreate()
    _solr5_tp = sc._jvm.com.amazon.productads.latac.udfs.Solr5AnalyzerPySparkUDF.solr5TokenizeUdf()
    return Column(_solr5_tp.apply(_to_seq(sc, [text, marketplace_id], _to_java_column)))


def normalize_dataset(dataset):
    normalized_dataset = (
        dataset
            #.withColumn("normalized_tokens", solr5_tokenize(col("tokens"), col("marketplace_id")))
            .withColumn("marketplace_id_str", col("marketplace_id").cast("string"))
            .withColumn("normalized_tokens", expr("analyzerUdf(tokens, marketplace_id_str)"))
            .drop("marketplace_id", "tokens", "marketplace_id_str")
            .withColumnRenamed("normalized_tokens", "tokens")
    )

    logger.info("#####################")
    logger.info("Normalize dataset")
    logger.info("#####################")
    normalized_dataset.show(10, False)

    return normalized_dataset


'''
1. Calculate match rates
2. Calculate match rates improved
'''
def main():
    # Initialization
    args = process_args()

    marketplace = get_marketplace_name(args.marketplace_id).upper()

    # Register Solr8 Tokenizer UDF
    spark.udf.registerJavaFunction("analyzerUdf", "com.amazon.productads.latac.udfs.Solr8LuceneAnalyzer")

    # Match set date format (2022-12-11)
    formatted_match_set_date = format_date(args.match_set_on_date, "%Y-%m-%d")
    s3_match_set_folder = f"{s3_match_set_prefix}{marketplace}/{formatted_match_set_date}/match_set/"

    daily_match_set = spark.read.parquet(s3_match_set_folder + "daily")
    daily_tail_match_set = spark.read.parquet(s3_match_set_folder + "daily_tail")
    segment_match_set = spark.read.parquet(s3_match_set_folder + "segment")

    # Calculate match rates for a marketplace
    if args.S3_dataset.lower() == "none":
        # Calculate metrics for daily_match_set
        logger.info("#################################################")
        logger.info("Calculate metrics for daily_match_set")
        logger.info("#################################################")
        calculate_index_metrics(daily_match_set)
        calculate_request_metrics(daily_match_set)

        # Calculate metrics for daily_tail_match_set
        logger.info("#################################################")
        logger.info("Calculate metrics for daily_tail_match_set")
        logger.info("#################################################")
        calculate_index_metrics(daily_tail_match_set)

        # Calculate metrics for daily_segment_match_set
        logger.info("#################################################")
        logger.info("Calculate metrics for daily_segment_match_set")
        logger.info("#################################################")
        calculate_query_metrics(segment_match_set)

    else: # Calculate match rate improvement for a new dataset
        if not s3_path_exists(sc, args.S3_dataset):
            logger.info(f"{args.S3_dataset} does not exists, return")
            return

        # Load new dataset
        if args.S3_dataset_format == "parquet":
            dataset = spark.read.parquet(args.S3_dataset)
        else:
            dataset = spark.read.json(args.S3_dataset)
        dataset = (
            dataset
                .toDF('asin', 'marketplace_id', 'tokens')
                .withColumn("marketplace_id_long", col("marketplace_id").cast(LongType()))
                .where(col("marketplace_id_long") == args.marketplace_id)
                .drop("marketplace_id")
                .withColumnRenamed("marketplace_id_long", "marketplace_id")
        )

        if args.require_tokenization:
            normalized_dataset = normalize_dataset(dataset)
        else:
            normalized_dataset = dataset

        # Calculate metrics for daily_match_set
        logger.info("#################################################")
        logger.info("Calculate improved metrics for daily_match_set")
        logger.info("#################################################")
        daily_joined_set = join_matchset_dataset(daily_match_set, normalized_dataset, args.join_with_matchset)
        calculate_index_metrics_improved(args, daily_joined_set)

        # Calculate metrics for daily_tail_match_set
        logger.info("#####################################################")
        logger.info("Calculate improved metrics for daily_tail_match_set")
        logger.info("#####################################################")
        daily_tail_joined_set = join_matchset_dataset(daily_tail_match_set, normalized_dataset, args.join_with_matchset)
        calculate_index_metrics_improved(args, daily_tail_joined_set)

        # Calculate metrics for daily_segment_match_set
        logger.info("########################################################")
        logger.info("Calculate improved metrics for daily_segment_match_set")
        logger.info("########################################################")
        segment_joined_set = join_matchset_dataset(segment_match_set, normalized_dataset, args.join_with_matchset)
        calculate_query_metrics_improved(segment_joined_set)

    logger.info(f"Match set - match rate improved calculation is done")


if __name__ == "__main__":
    main()
