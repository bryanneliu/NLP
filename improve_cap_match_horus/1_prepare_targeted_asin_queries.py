import warnings
import logging, sys
import subprocess
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, max, lit, broadcast, collect_list, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from marketplace_utils import get_marketplace_name

from tokenizer_enhanced import (
    set_spear_tokenizer_resources, normalize_queries
)

from match_commons import (
    SOURCE_CAP, SOURCE_TOMMY_ADDITION,
    get_args, set_args_paths, s3_path_exists, register_horus_tokenization_match_udf
)

warnings.simplefilter(action="ignore", category=FutureWarning)
logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.INFO)

spark = (
    SparkSession.builder.master("yarn")
    .appName("Prepare-targeted-asin-queries")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext

Janus_snapshot_S3_prefix = {
    "EU": "s3://cpp-janus-eu-prod-sp-redrive-json/EUAmazon/",
    "FE": "s3://cpp-janus-fe-prod-sp-redrive-json/FEAmazon/",
    "NA": "s3://cpp-janus-na-prod-sp-redrive-json/USAmazon/"
}

# s3://cpp-janus-na-prod-sp-redrive-json/USAmazon/2023/06/18/
def load_janus_data(region, marketplace_id, date):
    date_str = date.strftime("%Y/%m/%d")
    janus_folder = Janus_snapshot_S3_prefix[region] + f"{date_str}"
    if not s3_path_exists(sc, janus_folder):
        return None

    janus = spark.read.json(f"{janus_folder}/*/update*.gz")
    janus = (
        janus
            .where(col("marketplaceid") == marketplace_id)
            .select("asin", "averageRating", "reviewCount")
    )
    return janus


def handle_none_dataframe(df):
    # Define the schema for empty dataframe
    empty_schema = StructType([
        StructField('asin', StringType(), nullable=True),
        StructField('averageRating', DoubleType(), nullable=True),
        StructField('reviewCount', DoubleType(), nullable=True),
    ])
    return spark.createDataFrame([], empty_schema) if df is None else df


def get_latest_3_days_janus_asins(args):
    region = args.region.upper()

    current_date = datetime.date.today()
    janus_current = load_janus_data(region, args.marketplace_id, current_date)
    previous_date = current_date - datetime.timedelta(days=1)
    janus_previous = load_janus_data(region, args.marketplace_id, previous_date)
    next_date = current_date + datetime.timedelta(days=1)
    janus_next = load_janus_data(region, args.marketplace_id, next_date)

    janus = (
        handle_none_dataframe(janus_previous)
            .unionAll(handle_none_dataframe(janus_current))
                .unionAll(handle_none_dataframe(janus_next))
    )

    janus_asins = (
        janus
            .groupBy("asin")
            .agg(
                max("averageRating").alias("averageRating"),
                max("reviewCount").alias("reviewCount")
            )
    )

    if args.verbose_mode:
        logger.info(f"Janus unique ASIN count: {janus_asins.count()}")

    janus_asins = janus_asins.coalesce(500)
    janus_asins.write.mode("overwrite").parquet(args.s3_janus_asins_latest)


def prepare_targeted_asin_queries(spark, args, logger):
    # cap & tommy addition <query, ASINs>
    cap = spark.read.parquet(args.s3_cap_latest)
    cap = cap.withColumn("source", lit(SOURCE_CAP))
    tommy_addition = spark.read.parquet(args.s3_tommy_addition_latest)
    tommy_addition = tommy_addition.withColumn("source", lit(SOURCE_TOMMY_ADDITION))
    query_asin = cap.unionAll(tommy_addition)

    if args.verbose_mode:
        unique_queries = (
            query_asin
                .select("query", "normalized_query", "broad_normalized_query", "has_num_pattern")
                .distinct()
        )
        logger.info(f"CAP and Tommy addition - unique query count: {unique_queries.count()}")

        num_queries = unique_queries.where(col("has_num_pattern") == 1)
        logger.info(f"CAP and Tommy addition - unique num query count: {num_queries.count()}")

    # Targeted Janus ASINs
    janus_asins = spark.read.parquet(args.s3_janus_asins_latest)
    head_asins = janus_asins.where(col("reviewCount") >= args.review_count_threshold).select("asin")

    # Targeted <query, ASINs>
    query_asin = query_asin.join(broadcast(head_asins), ["asin"], "inner")

    if args.verbose_mode:
        logger.info(f"Head ad asin count with review count >= {args.review_count_threshold}: {head_asins.count()}")
        logger.info(f"<Query, ad ASIN> count with head asin filtering: {query_asin.count()}")

    # Targeted <ASIN, queries>
    # Performance Improvement: Prepare the input for broad/exact/phrase/close matches
    asin_queries = query_asin.groupBy("asin").agg(
        collect_list(
            struct("query", "cap_score", "source", "txt_norm_query", "normalized_query", "broad_normalized_query", "has_num_pattern")
        ).alias("query_info_list")
    )

    asin_queries = asin_queries.coalesce(500)
    asin_queries.write.mode("overwrite").parquet(args.s3_targeted_asin_queries)


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


def construct_base_cap(spark, args, logger):
    s3_prefix = f's3://sourcing-cap-features-prod/projects/cap/region={args.region}'

    # Get the latest blend_cap folder
    s3_blend_cap_folder = f"{s3_prefix}/blend_cap/marketplace_id={args.marketplace_id}/"
    cap_latest_ds = get_cap_latest_ds(s3_blend_cap_folder)
    s3_latest_blend_cap_folder = f"{s3_blend_cap_folder}date={cap_latest_ds}/"
    logger.info(f"s3_latest_blend_cap_folder: {s3_latest_blend_cap_folder}")

    blend_cap = spark.read.parquet(s3_latest_blend_cap_folder)
    blend_cap = blend_cap.where(col("clicks") > 1)
    blend_cap = blend_cap.select(["query", "asin", "cap_score"])

    # Get the latest blend_cap_knapsack folder
    s3_blend_cap_knapsack_folder = f"{s3_prefix}/blend_cap_knapsack/marketplace_id={args.marketplace_id}/"
    cap_latest_ds = get_cap_latest_ds(s3_blend_cap_knapsack_folder)
    s3_latest_blend_cap_knapsack_folder = f"{s3_blend_cap_knapsack_folder}date={cap_latest_ds}/"
    logger.info(f"s3_latest_blend_cap_knapsack_folder: {s3_latest_blend_cap_knapsack_folder}")

    blend_cap_knapsack = spark.read.parquet(s3_latest_blend_cap_knapsack_folder)
    blend_cap_knapsack = blend_cap_knapsack.select(["query", "asin", "cap_score"])

    # Merge blend_cap and blend_cap_knapsack
    cap = blend_cap.union(blend_cap_knapsack)
    cap = cap.groupBy("query", "asin").agg(
        max("cap_score").alias("cap_score")
    )

    cap = cap.withColumn("searchQuery", regexp_replace("query", "_", " ")).drop("query").withColumnRenamed("searchQuery", "query")

    if args.verbose_mode:
        q_cnt = cap.select("query").distinct().count()
        qa_cnt = cap.select("query", "asin").distinct().count()
        logger.info(f"latest 1-year CAP, unique query count:{q_cnt}, unique <query, asin> pair count: {qa_cnt}")

    return cap


'''
Get latest 1-year CAP and Tommy diff:
query_asin_diff = 502871795
unique_query_diff = 40433168

root
 |-- asin: string (nullable = true)
 |-- sp_impressions: double (nullable = true)
 |-- impressions: long (nullable = true)
 |-- pos: double (nullable = true)
 |-- clicks: long (nullable = true)
 |-- adds: long (nullable = true)
 |-- purchases: long (nullable = true)
 |-- consumes: long (nullable = true)
 |-- query_freq: long (nullable = true)
 |-- raw_query: string (nullable = true)
'''
def load_tommy_raw_engaged_and_cap_diff(args):
    query_asin_diff = spark.read.parquet(args.s3_tommy_addition)

    query_asin = (
        query_asin_diff
            .withColumn(
                "cap_score",
                col("clicks") * 0.0007 + col("adds") * 0.02 + col("purchases") * 1.4 + col("consumes") * 1.4)
            .withColumnRenamed("raw_query", "query")
            .select("query", "asin", "cap_score")
            .groupBy("query", "asin")
            .agg(max("cap_score").alias("cap_score"))
    )

    if args.verbose_mode:
        logger.info(f"Tommy raw engaged and CAP <query, asin> diff: {query_asin.count()}")
        unique_query_diff = query_asin.select("query").distinct().count()
        logger.info(f"Tommy raw engaged and CAP unique query diff: {unique_query_diff}")

    return query_asin


def main():
    args = get_args()
    logger.info(f"Prepare targeted_asin_queries Start, arguments are {args}")

    marketplace_name = get_marketplace_name(args.marketplace_id)
    logger.info(f"CAP marketplace name: {marketplace_name}")

    set_args_paths(args, logger)
    args.s3_tommy_addition = (
            "s3://spear-match-improvement/" +
            marketplace_name.lower() +
            "/cap/raw_engaged_cap_diff/"
    )

    '''
    Register old and new tokenizer
    '''
    register_horus_tokenization_match_udf(spark)
    set_spear_tokenizer_resources(spark)

    # Refresh CAP
    cap = construct_base_cap(spark, args, logger)
    normalized_cap = normalize_queries(spark, args, logger, cap)
    normalized_cap = normalized_cap.coalesce(500)
    normalized_cap.write.mode("overwrite").parquet(args.s3_cap_latest)
    logger.info(f"{args.s3_cap_latest} has been updated")

    # Refresh Tommy_addition
    tommy_addition = load_tommy_raw_engaged_and_cap_diff(args)
    normalized_tommy_addition = normalize_queries(spark, args, logger, tommy_addition)
    normalized_tommy_addition = normalized_tommy_addition.coalesce(500)
    normalized_tommy_addition.write.mode("overwrite").parquet(args.s3_tommy_addition_latest)
    logger.info(f"{args.s3_tommy_addition_latest} has been updated")

    # Get latest Janus ASINs
    get_latest_3_days_janus_asins(args)

    # Prepare targeted_asin_queries
    prepare_targeted_asin_queries(spark, args, logger)


if __name__ == "__main__":
    main()

'''
Performance Improvement:
1. Prepare the common input asin_queries for broad/phrase/exact/close matches
2. Support "refresh_latest" to avoid un-necessary fresh 
'''

# In PySpark, coalesce() function is used to reduce the number of partitions in a DataFrame or RDD.
# It merges existing partitions to form new partitions.
# The coalesce() function is useful when you want to decrease the number of partitions without performing a full shuffle.