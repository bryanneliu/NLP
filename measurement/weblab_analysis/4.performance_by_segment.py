import argparse
import logging
import subprocess

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, count, avg, round, when, sum, lit, concat, countDistinct, percent_rank
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

from datetime import timedelta, datetime

import common as cm
from marketplace_utils import (
    get_marketplace_name,
    get_region_name
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

spark = (
    SparkSession.builder.master("yarn")
    .appName("performance_by_segment")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext


def unique_asin_advertiser_analysis(weblab):
    analysis = weblab.groupBy("treatment").agg(
        countDistinct("asin").alias("unique_asin_count"),
        countDistinct("advertiser_id").alias("unique_advertiser_count")
    )

    analysis.show(5, False)


def get_weblab_percentile(weblab):
    # Query request_count percentile_rank
    query_request_count = weblab.groupBy("search_query").agg(
        count("http_request_id").alias("request_count")
    )

    order_score = Window.orderBy(col("request_count"))
    query_request_count_percentile = (
        query_request_count
        .withColumn("query_request_count_percentile", percent_rank().over(order_score))
    ).cache()

    # ASIN impression_count percentile_rank
    asin_impression_count = weblab.groupBy("asin").agg(
        count("*").alias("asin_impression_count")
    )

    order_score = Window.orderBy(col("asin_impression_count"))
    asin_impression_count_percentile = (
        asin_impression_count
        .withColumn("asin_impression_count_percentile", percent_rank().over(order_score))
    ).cache()

    # ASIN click_count percentile_rank
    asin_click_count = weblab.where(col("click") > 0).groupBy("asin").agg(
        sum("click").alias("asin_click_count")
    )

    order_score = Window.orderBy(col("asin_click_count"))
    asin_click_count_percentile = (
        asin_click_count
        .withColumn("asin_click_count_percentile", percent_rank().over(order_score))
    ).cache()

    weblab_percentile = (
        weblab.join(query_request_count_percentile, ["search_query"], "inner")
        .join(asin_impression_count_percentile, ["asin"], "inner")
        .join(asin_click_count_percentile, ["asin"], "left")
        .withColumn(
            "normalized_asin_click_count_percentile",
            when(col("asin_click_count_percentile").isNull(), 0).otherwise(col("asin_click_count_percentile"))
        )
        .withColumn(
            "query_segment",
            when(col("query_request_count_percentile") >= 0.66, "head").otherwise(
                when(col("query_request_count_percentile") >= 0.34, "torso").otherwise("tail"))
        )
        .withColumn(
            "asin_impression_segment",
            when(col("asin_impression_count_percentile") >= 0.75,
                 "asin_impression_count_top_75th_percentile").otherwise(
                when(col("asin_impression_count_percentile") >= 0.5,
                     "asin_impression_count_50th_to_75th_percentile").otherwise(
                    when(col("asin_impression_count_percentile") >= 0.25,
                         "asin_impression_count_25th_to_50th_percentile").otherwise(
                        "asin_impression_count_bottom_25th_percentile")
                )
            )
        )
        .withColumn(
            "asin_click_segment",
            when(col("normalized_asin_click_count_percentile") >= 0.75,
                 "asin_click_count_top_75th_percentile").otherwise(
                when(col("normalized_asin_click_count_percentile") >= 0.5,
                     "asin_click_count_50th_to_75th_percentile").otherwise(
                    when(col("normalized_asin_click_count_percentile") >= 0.25,
                         "asin_click_count_25th_to_50th_percentile").otherwise(
                        when(col("normalized_asin_click_count_percentile") > 0,
                             "asin_click_count_bottom_25th_percentile").otherwise(
                            "no_click")
                    )
                )
            )
        )
        .withColumn(
            "asin_review_segment",
            when(col("reviewCount") >= 100000, "asin_review_count_more_than_100k").otherwise(
                when(col("reviewCount") >= 50000, "asin_review_count_50k_to_100k").otherwise(
                    when(col("reviewCount") >= 10000, "asin_review_count_10k_to_50k").otherwise(
                        when(col("reviewCount") >= 5000, "asin_review_count_5k_to_10k").otherwise(
                            when(col("reviewCount") >= 1000, "asin_review_count_1k_to_5k").otherwise(
                                when(col("reviewCount") >= 500, "asin_review_count_500_to_1k").otherwise(
                                    when(col("reviewCount") >= 200, "asin_review_count_200_to_500").otherwise(
                                        when(col("reviewCount") >= 100, "asin_review_count_100_to_200").otherwise(
                                            when(col("reviewCount") >= 50, "asin_review_count_50_to_100").otherwise(
                                                when(col("reviewCount") >= 10, "asin_review_count_10_to_50").otherwise(
                                                    when(col("reviewCount") >= 1,
                                                         "asin_review_count_1_to_10").otherwise(
                                                        "asin_review_count_0")
                                                ))))))))))
        )
        .withColumn(
            "asin_rating_segment",
            when(col("averageRating") == 5, "asin_rating_5").otherwise(
                when(col("averageRating") >= 4, "asin_rating_4_to_5").otherwise(
                    when(col("averageRating") >= 3, "asin_rating_3_to_4").otherwise(
                        when(col("averageRating") >= 2, "asin_rating_2_to_3").otherwise(
                            when(col("averageRating") >= 1, "asin_rating_1_to_2").otherwise(
                                when(col("averageRating") > 0, "asin_rating_0_to_1").otherwise(
                                    "asin_rating_0"))))))
        )
    )

    return weblab_percentile


def calculate_segment_distribution(weblab_percentile, segment, s3_output):
    segment_distribution = weblab_percentile.groupBy("treatment", segment).agg(
        F.count('*').alias('impressions'),
        round(sum("revenue"), 4).alias("sum_rev"),
        round(count("asin"), 4).alias("asin_count"),
        round(sum("click"), 4).alias("clicks"),
        round(avg("click"), 4).alias("actr"),
        round(avg("p_irrel"), 4).alias("avg_p_irrel"),
        round(avg("ECTR"), 4).alias("avg_ectr"),
        round(avg("ECVR"), 4).alias("avg_ecvr"),
        round(avg("cpc"), 4).alias("avg_cpc"),
        round(sum("ops"), 4).alias("sum_ops"),
        round(sum("ops")*1.0/sum("revenue"), 4).alias("avg_roas"),
        round(F.count('*')*1.0/F.countDistinct("http_request_id"), 4).alias("ad_density"),
        F.countDistinct('asin', 'advertiser_id').alias('asin_advid_count')
    )

    segment_distribution.repartition(1).write.mode("overwrite").option("header", "true").csv(s3_output + "raw/" + segment)

    return segment_distribution


def calculate_segment_distribution_diff(segment_distribution, segment, s3_output):
    segment_distribution_baseline = (
        segment_distribution
            .where(col("treatment") == "T1")
            .drop("treatment")
            .withColumnRenamed("sum_rev", "baseline_sum_rev")
            .withColumnRenamed("impressions", "baseline_impressions")
            .withColumnRenamed("clicks", "baseline_clicks")
            .withColumnRenamed("avg_p_irrel", "baseline_avg_p_irrel")
            .withColumnRenamed("avg_ectr", "baseline_avg_ectr")
            .withColumnRenamed("avg_ecvr", "baseline_avg_ecvr")
            .withColumnRenamed("avg_cpc", "baseline_avg_cpc")
            .withColumnRenamed("actr", "baseline_actr")
    )

    segment_distribution_diff = (
        segment_distribution
            .join(segment_distribution_baseline, [segment], "inner")
            .withColumn("sum_rev_lift", (col("sum_rev")/col("baseline_sum_rev")-1))
            .withColumn("sum_rev_lift_percent", concat((col("sum_rev_lift")*100).cast(StringType()), lit("%")))
            .withColumn("impressions_lift", round(col("impressions")/col("baseline_impressions")-1, 6))
            .withColumn("impressions_lift_percent", concat((col("impressions_lift")*100).cast(StringType()), lit("%")))
            .withColumn("clicks_lift", round(col("clicks")/col("baseline_clicks")-1, 6))
            .withColumn("clicks_lift_percent", concat((col("clicks_lift")*100).cast(StringType()), lit("%")))
            .withColumn("avg_p_irrel_lift", round(col("avg_p_irrel")/col("baseline_avg_p_irrel")-1, 6))
            .withColumn("avg_p_irrel_lift_percent", concat((col("avg_p_irrel_lift")*100).cast(StringType()), lit("%")))
            .withColumn("avg_ectr_lift", round(col("avg_ectr")/col("baseline_avg_ectr")-1, 6))
            .withColumn("avg_ectr_lift_percent", concat((col("avg_ectr_lift")*100).cast(StringType()), lit("%")))
            .withColumn("avg_ecvr_lift", round(col("avg_ecvr")/col("baseline_avg_ecvr")-1, 6))
            .withColumn("avg_ecvr_lift_percent", concat((col("avg_ecvr_lift")*100).cast(StringType()), lit("%")))
            .withColumn("avg_cpc_lift", round(col("avg_cpc")/col("baseline_avg_cpc")-1, 6))
            .withColumn("avg_cpc_lift_percent", concat((col("avg_cpc_lift")*100).cast(StringType()), lit("%")))
            .withColumn("actr_lift", round(col("actr")/col("baseline_actr")-1, 6))
            .withColumn("actr_lift_percent", concat((col("actr_lift")*100).cast(StringType()), lit("%")))
            .select("treatment", segment, "sum_rev_lift_percent", "impressions_lift_percent", "clicks_lift_percent",
                    "avg_p_irrel_lift_percent", "avg_ectr_lift_percent", "avg_ecvr_lift_percent", "avg_cpc_lift_percent", "actr_lift_percent")
    ).sort(segment, "treatment")

    segment_distribution_diff.repartition(1).write.mode("overwrite").option("header", "true").csv(s3_output + "diff/" + segment)


def main():
    # Initialization
    args = cm.process_args()

    marketplace = get_marketplace_name(args.marketplace_id).upper()

    s3_output = f"{cm.s3_weblab_analysis}{args.Solr_or_Horus.lower()}/{args.weblab_name}/{marketplace}/performance_by_segment/"

    weblab = cm.load_impressed_ad_details(spark, args)

    logger.info("unique ASINs & unique advertisers:")
    unique_asin_advertiser_analysis(weblab)

    weblab_percentile = get_weblab_percentile(weblab)

    logger.info("query_segment_distribution:")
    segment = "query_segment"
    segment_distribution = calculate_segment_distribution(weblab_percentile, segment, s3_output)
    segment_distribution.sort(segment, "treatment").show(100, False)
    calculate_segment_distribution_diff(segment_distribution, segment, s3_output)

    logger.info("asin_impression_segment_distribution:")
    segment = "asin_impression_segment"
    segment_distribution = calculate_segment_distribution(weblab_percentile, segment, s3_output)
    segment_distribution.sort(segment, "treatment").show(100, False)
    calculate_segment_distribution_diff(segment_distribution, segment, s3_output)

    logger.info("asin_click_segment_distribution:")
    segment = "asin_click_segment"
    segment_distribution = calculate_segment_distribution(weblab_percentile, segment, s3_output)
    segment_distribution.sort(segment, "treatment").show(100, False)
    calculate_segment_distribution_diff(segment_distribution, segment, s3_output)

    logger.info("asin_review_segment_distribution:")
    segment = "asin_review_segment"
    segment_distribution = calculate_segment_distribution(weblab_percentile, segment, s3_output)
    segment_distribution.sort(segment, "treatment").show(100, False)
    calculate_segment_distribution_diff(segment_distribution, segment, s3_output)

    logger.info("asin_rating_segment_distribution:")
    segment = "asin_rating_segment"
    segment_distribution = calculate_segment_distribution(weblab_percentile, segment, s3_output)
    segment_distribution.sort(segment, "treatment").show(100, False)
    calculate_segment_distribution_diff(segment_distribution, segment, s3_output)


if __name__ == "__main__":
    main()

