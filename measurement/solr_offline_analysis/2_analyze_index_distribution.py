import subprocess
import logging
import sys

import numpy as np

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, size, explode, count, sum, udf, split, countDistinct, max, lit, when, avg, percentile_approx

from schemas import *

from marketplace_utils import get_region_name

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
    .appName("analyze_index_distribution")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext


'''
Get latest 1-year CAP <query, ASIN>
'''
def get_cap_latest_ds(s3_cap_folder):
    ls_output = str(subprocess.check_output(['aws', 's3', 'ls', s3_cap_folder]).decode('UTF-8'))
    split_outputs = ls_output.strip().split()
    snapshots = sorted([snapshot for snapshot in split_outputs if 'PRE' not in snapshot])
    # snapshots looks like ['date=2022-08-29/', 'date=2022-09-14/']
    cap_latest_ds = snapshots[-1][5:-1]
    return cap_latest_ds


# kw_searches is query specific
# searches is <query, ASIN> specific
def construct_base_cap(marketplace_id, region):
    # s3://sourcing-cap-features-prod/projects/cap/region=EU/
    s3_prefix = f's3://sourcing-cap-features-prod/projects/cap/region={region}'

    # Get the latest blend_cap folder
    s3_blend_cap_folder = f"{s3_prefix}/blend_cap/marketplace_id={marketplace_id}/"
    cap_latest_ds = get_cap_latest_ds(s3_blend_cap_folder)
    s3_latest_blend_cap_folder = f"{s3_blend_cap_folder}date={cap_latest_ds}/"
    logger.info(f"s3_latest_blend_cap_folder: {s3_latest_blend_cap_folder}")

    blend_cap = spark.read.parquet(s3_latest_blend_cap_folder)
    blend_cap = blend_cap.where(col("clicks") > 1)
    blend_cap = blend_cap.select(
        ["query", "asin", "kw_searches", "searches", "clicks", "adds", "consumes", "purchases"])

    # Get the latest blend_cap_knapsack folder
    s3_blend_cap_knapsack_folder = f"{s3_prefix}/blend_cap_knapsack/marketplace_id={marketplace_id}/"
    cap_latest_ds = get_cap_latest_ds(s3_blend_cap_knapsack_folder)
    s3_latest_blend_cap_knapsack_folder = f"{s3_blend_cap_knapsack_folder}date={cap_latest_ds}/"
    logger.info(f"s3_latest_blend_cap_knapsack_folder: {s3_latest_blend_cap_knapsack_folder}")

    blend_cap_knapsack = spark.read.parquet(s3_latest_blend_cap_knapsack_folder)
    blend_cap_knapsack = blend_cap_knapsack.select(
        ["query", "asin", "kw_searches", "searches", "clicks", "adds", "consumes", "purchases"])

    # Merge blend_cap and blend_cap_knapsack
    cap = blend_cap.unionAll(blend_cap_knapsack)
    cap = cap.groupBy("query", "asin").agg(
        max("kw_searches").alias("kw_searches"),
        max("clicks").alias("clicks"),
        max("adds").alias("adds"),
        max("consumes").alias("consumes"),
        max("purchases").alias("purchases"),
    )

    asin_engagement = cap.groupBy("asin").agg(
        countDistinct("query").alias("distinct_query_count"),
        sum("kw_searches").alias("impression_count"),
        sum("clicks").alias("click_count"),
        sum("purchases").alias("purchase_count")
    )

    logger.info(f"Engaged ASINs in 1-year CAP: {asin_engagement.count()}")
    return asin_engagement


Janus_snapshot_S3_prefix = {
    "EU": "s3://cpp-janus-eu-prod-sp-redrive-json/EUAmazon/",
    "FE": "s3://cpp-janus-fe-prod-sp-redrive-json/FEAmazon/",
    "NA": "s3://cpp-janus-na-prod-sp-redrive-json/USAmazon/"
}
# s3://cpp-janus-na-prod-sp-redrive-json/USAmazon/2023/06/18/
def get_asin_reviews(s3_janus, marketplace_id):
    janus = spark.read.json(f"{s3_janus}/*/update*.gz")
    asin_reviews = (
        janus
            .where(col("marketplaceid") == marketplace_id)
            .select("asin", "averageRating", "reviewCount")
            .groupBy("asin").agg(
                max("averageRating").alias("averageRating"),
                max("reviewCount").alias("reviewCount")
            )
    )
    return asin_reviews


def load_indexable(s3_indexable, marketplace_id):
    logger.info(f"s3_indexable: {s3_indexable}")
    indexable = spark.read.format('json').load(s3_indexable, schema=INDEXABLE_PROJECTED_SCHEMA)
    indexable = indexable.where(col("marketplaceId") == f'{marketplace_id}')
    return indexable


def get_solr_index(indexable):
    solr_index = (
        indexable
            .select(explode("ad"))
            .select("col.asin", "col.glProductGroupType", "col.brand", "col.title", "col.expression")
            .distinct()
            .withColumn("title_tok_cnt", size(split(col("title"), " ")))
            .withColumn("index_tok_cnt", size(split(col("expression"), " ")))
    )

    logger.info(f"Solr asin count: {solr_index.count()}")
    return solr_index


# Hot/Cold ASINs for advertising
# Token distribution on Overall/GL/Popularity
# Calculate the popular CAP asins with low token coverage
def calculate_distribution(solr_index):
    title_token_count_distribution = (
        solr_index.select(percentile_approx("title_tok_cnt", np.arange(0.95, 0.0, -0.05).tolist(), 1000))
    )
    logger.info("title_token_count_distribution:")
    title_token_count_distribution.show(5, False)

    index_token_count_distribution = (
        solr_index.select(percentile_approx("index_tok_cnt", np.arange(0.95, 0.0, -0.05).tolist(), 1000))
    )
    logger.info("index_token_count_distribution:")
    index_token_count_distribution.show(5, False)

    distinct_query_count_distribution = (
        solr_index.select(percentile_approx("distinct_query_count", np.arange(0.95, 0.0, -0.05).tolist(), 1000))
    )
    logger.info("distinct_query_count_distribution:")
    distinct_query_count_distribution.show(5, False)

    impression_count_distribution = (
        solr_index.select(percentile_approx("impression_count", np.arange(0.95, 0.0, -0.05).tolist(), 1000))
    )
    logger.info("impression_count_distribution:")
    impression_count_distribution.show(5, False)

    click_count_distribution = (
        solr_index.select(percentile_approx("click_count", np.arange(0.95, 0.0, -0.05).tolist(), 1000))
    )
    logger.info("click_count_distribution:")
    click_count_distribution.show(5, False)

    purchase_count_distribution = (
        solr_index.select(percentile_approx("purchase_count", np.arange(0.95, 0.0, -0.05).tolist(), 1000))
    )
    logger.info("purchase_count_distribution:")
    purchase_count_distribution.show(5, False)

    review_count_distribution = (
        solr_index.select(percentile_approx("reviewCount", np.arange(0.95, 0.0, -0.05).tolist(), 1000))
    )
    logger.info("review_count_distribution:")
    review_count_distribution.show(5, False)

    avg_rating_distribution = (
        solr_index.select(percentile_approx("averageRating", np.arange(0.95, 0.0, -0.05).tolist(), 1000))
    )
    logger.info("avg_rating_distribution:")
    avg_rating_distribution.show(5, False)


# Generate ASIN index, index statistics, and ASIN popularity
def main():
    # Initialization
    args = process_args()

    if args.task.lower() != "index_distribution":
        return

    # Get asin, distinct_query_count, impression_count, click_count, purchase_count
    region = get_region_name(args.marketplace_id).upper()
    asin_engagement = construct_base_cap(args.marketplace_id, region)

    # Get asin, reviewCount, averageRating
    asin_reviews = get_asin_reviews(args.s3_janus, args.marketplace_id)

    # Get asin, glProductGroupType, brand, title, expression, title_tok_cnt, index_tok_cnt
    indexable = load_indexable(args.s3_indexable, args.marketplace_id)
    solr_index = get_solr_index(indexable)
    solr_index = (
        solr_index
            .join(asin_engagement, ["asin"], "left")
            .join(asin_reviews, ["asin"], "left")
    )

    calculate_distribution(solr_index)


if __name__ == "__main__":
    main()
