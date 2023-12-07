import argparse
import logging
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round, when, sum, lit, coalesce

from datetime import timedelta, datetime

import common as cm
from s3_utils import SnapshotMeta, S3Utils
from marketplace_utils import (
    get_marketplace_name,
    get_region_name
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

spark = (
    SparkSession.builder.master("yarn")
    .appName("cold_asin_metrics")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext


def get_review_distribution(data):
    added = data.withColumn("reviewCountEq0", when(col("reviewCount") == 0, 1).otherwise(0))
    added = added.withColumn("reviewCountLess5", when(col("reviewCount") <= 5, 1).otherwise(0))
    added = added.withColumn("reviewCountLess10", when(col("reviewCount") <= 10, 1).otherwise(0))
    added = added.withColumn("reviewCountLess100", when(col("reviewCount") <= 100, 1).otherwise(0))
    added = added.withColumn("ratingLess1", when(col("averageRating") <= 1, 1).otherwise(0))
    added = added.withColumn("ratingLess2", when(col("averageRating") <= 2, 1).otherwise(0))
    added = added.withColumn("ratingLess3", when(col("averageRating") <= 3, 1).otherwise(0))

    statistics = added.groupBy("treatment").agg(
        count("asin").alias("asin_count"),
        avg("averageRating").alias("avg_rating"),
        avg("reviewCount").alias("avg_reviewCnt"),
        sum("reviewCountEq0").alias("reviewCountEq0Cnt"),
        sum("reviewCountLess5").alias("reviewCountLess5Cnt"),
        sum("reviewCountLess10").alias("reviewCountLess10Cnt"),
        sum("reviewCountLess100").alias("reviewCountLess100Cnt"),
        sum("ratingLess1").alias("ratingLess1Cnt"),
        sum("ratingLess2").alias("ratingLess2Cnt"),
        sum("ratingLess3").alias("ratingLess3Cnt"),
    )

    overall = statistics.select("treatment", "asin_count", "avg_rating", "avg_reviewCnt")
    logger.info("Treatment all impression ASINs - avg_rating and avg_reviewCount:")
    overall.show(10, False)

    review = statistics.withColumn("reviewCountEq0Ratio", round(col("reviewCountEq0Cnt") / col("asin_count"), 4))
    review = review.withColumn("reviewCountLess5Ratio", round(col("reviewCountLess5Cnt") / col("asin_count"), 4))
    review = review.withColumn("reviewCountLess10Ratio", round(col("reviewCountLess10Cnt") / col("asin_count"), 4))
    review = review.withColumn("reviewCountLess100Ratio", round(col("reviewCountLess100Cnt") / col("asin_count"), 4))

    review = review.select("treatment", "reviewCountEq0Ratio", "reviewCountLess5Ratio",
                           "reviewCountLess10Ratio", "reviewCountLess100Ratio")

    logger.info("Treatment all impression ASINs ratio with low review count:")
    review.sort("treatment").show(10, False)

    rating = statistics.withColumn("ratingLess1Ratio", round(col("ratingLess1Cnt") / col("asin_count"), 4))
    rating = rating.withColumn("ratingLess2Ratio", round(col("ratingLess2Cnt") / col("asin_count"), 4))
    rating = rating.withColumn("ratingLess3Ratio", round(col("ratingLess3Cnt") / col("asin_count"), 4))

    rating = rating.select("treatment", "ratingLess1Ratio", "ratingLess2Ratio", "ratingLess3Ratio")
    logger.info("Treatment all impression ASINs ratio with low rating:")
    rating.sort("treatment").show(10, False)


def CAP_overlap(data, cap):
    cap_asins = cap.groupBy("asin").agg(
        sum("searches").alias("searches")
    )

    impressionJoinCAP = data.select("treatment", "asin").join(cap_asins, ["asin"], "left")
    impressionJoinCAP = impressionJoinCAP.withColumn("inCAP", when(col("searches").isNotNull(), 1).otherwise(0))
    impressionJoinCAP = impressionJoinCAP.withColumn("searchesEq1", when(col("searches") == 1, 1).otherwise(0))
    impressionJoinCAP = impressionJoinCAP.withColumn("searchesB1S10", when((col("searches") <= 10) & (col("searches") > 0), 1).otherwise(0))
    impressionJoinCAP = impressionJoinCAP.withColumn("searchesB10S100", when((col("searches") <= 100) & (col("searches") > 10), 1).otherwise(0))
    impressionJoinCAP = impressionJoinCAP.withColumn("searchesB100S1000", when((col("searches") <= 1000) & (col("searches") > 100), 1).otherwise(0))
    impressionJoinCAP = impressionJoinCAP.withColumn("low_searches", when(col("searches") <= 1000, col("searches")).otherwise(0))

    impressionJoinCAP_total_asin = impressionJoinCAP.groupBy("treatment").agg(count("asin").alias("total_asin_count"))

    logger.info("Treatment all impression ASINs overlapped with CAP:")
    impressionJoinCAP.groupBy("treatment", "inCAP").agg(
        count("asin").alias("asin_count")
    ).join(
        impressionJoinCAP_total_asin, "treatment"
    ).select(
        "treatment", "inCAP", col("asin_count") / col("total_asin_count")
    ).where(col("inCAP") == lit(1)).sort("treatment", "inCAP").show(10, False)

    impressionJoinCAP_total = impressionJoinCAP.where(col("inCAP") == lit(1)).groupBy("treatment").agg(
        sum(coalesce(col("searches"), lit(0))).alias("total_searches"),
        sum("low_searches").alias("total_low_searches"),
        count("asin").alias("asin_count"),
        round(sum("searchesEq1") / count("asin"), 6).alias("asinRatioSearchesEq1"),
        round(sum("searchesB1S10") / count("asin"), 6).alias("asinRatioSearchesB1S10"),
        round(sum("searchesB10S100") / count("asin"), 6).alias("total_searchesB10S100"),
        round(sum("searchesB100S1000") / count("asin"), 6).alias("total_searchesB100S1000"),
    )

    logger.info("Treatment all impression ASINs overlapped with CAP - Total ASIN searches & ASIN ratio with low searches:")
    impressionJoinCAP_total.sort("treatment").show(10, False)


'''
CAP_S3 = {
    "EU": "s3://sourcing-cap-features-prod/projects/cap/region=EU/blend_cap/",
    "FE": "s3://sourcing-cap-features-prod/projects/cap/region=FE/blend_cap/",
    "NA": "s3://sourcing-cap-features-prod/projects/cap/region=NA/blend_cap/"
}
'''
def construct_base_cap(marketplace_id, region):
    s3_prefix = f's3://sourcing-cap-features-prod/projects/cap/region={region}'
    ls_output = str(subprocess.check_output(['aws', 's3', 'ls', f"{s3_prefix}/blend_cap/marketplace_id={marketplace_id}/"]).decode('UTF-8'))
    split_outputs = ls_output.strip().split()
    snapshots = sorted([snapshot for snapshot in split_outputs if 'PRE' not in snapshot])
    # snapshots looks like ['date=2022-08-29/', 'date=2022-09-14/']
    cap_lastest_ds = snapshots[-1][5:-1]

    blend_cap = spark.read.parquet(f"{s3_prefix}/blend_cap/marketplace_id={marketplace_id}/date={cap_lastest_ds}/")
    blend_cap = blend_cap.where(col("clicks") > 1)
    blend_cap = blend_cap.select(["query", "kw_searches", "asin", "searches", "cap_score"])

    blend_cap_knapsack = spark.read.parquet(
        f"{s3_prefix}/blend_cap_knapsack/marketplace_id={marketplace_id}/date={cap_lastest_ds}/"
    )
    blend_cap_knapsack = blend_cap_knapsack.select(["query", "kw_searches", "asin", "searches", "cap_score"])

    df_cap = blend_cap.union(blend_cap_knapsack).distinct()

    return df_cap


def main():
    # Initialization
    args = cm.process_args()

    region = get_region_name(args.marketplace_id).upper()

    data = cm.load_impressed_ad_details(spark, args)

    get_review_distribution(data)

    df_cap = construct_base_cap(args.marketplace_id, region)
    CAP_overlap(data, df_cap)
    

if __name__ == "__main__":
    main()
