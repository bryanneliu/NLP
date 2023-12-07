import argparse
import logging
import subprocess
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, collect_list, struct, regexp_replace, expr, sum, min

from common import (
    s3_working_folder,
    format_date, s3_path_exists,
    remove_matched_queries_UDF,
)

from marketplace_utils import (
    get_marketplace_name, get_region_name
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
    .appName("get_not_matched_query_set")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext


def process_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--marketplace_id",
        type=int,
        default=1,
        help="Specify solr_index marketplaceid",
        required=True
    )

    parser.add_argument(
        "--solr_index_latest_ds",
        type=str,
        default="2022-12-11",
        help="Specify solr_index ds",
        required=True
    )

    parser.add_argument(
        "--use_cap_as_context",
        type=int,
        default=0,
        help="""Use CAP as context, if set to 0, use Tommy engaged as context""",
        required=False,
    )

    args, _ = parser.parse_known_args()

    return args


def load_solr_index(marketplace_id, s3_solr_index):
    solr_index = spark.read.parquet(s3_solr_index)
    solr_index = (
        solr_index
            .where(col("marketplaceid") == marketplace_id)
            .select("asin", "title_tokens", "index_tokens")
    )
    solr_index_asin = solr_index.select("asin")
    solr_index_asin.cache()
    logger.info(f"solr_index unique ASIN count: {solr_index_asin.count()}")
    return solr_index_asin, solr_index


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
        ["query", "asin", "cap_score", "kw_searches", "searches", "clicks", "adds", "consumes", "purchases"])

    # Get the latest blend_cap_knapsack folder
    s3_blend_cap_knapsack_folder = f"{s3_prefix}/blend_cap_knapsack/marketplace_id={marketplace_id}/"
    cap_latest_ds = get_cap_latest_ds(s3_blend_cap_knapsack_folder)
    s3_latest_blend_cap_knapsack_folder = f"{s3_blend_cap_knapsack_folder}date={cap_latest_ds}/"
    logger.info(f"s3_latest_blend_cap_knapsack_folder: {s3_latest_blend_cap_knapsack_folder}")

    blend_cap_knapsack = spark.read.parquet(s3_latest_blend_cap_knapsack_folder)
    blend_cap_knapsack = blend_cap_knapsack.select(
        ["query", "asin", "cap_score", "kw_searches", "searches", "clicks", "adds", "consumes", "purchases"])

    # Merge blend_cap and blend_cap_knapsack
    cap = blend_cap.unionAll(blend_cap_knapsack)
    cap = cap.withColumn("searchQuery", regexp_replace("query", "_", " "))
    cap = cap.withColumn("normalized_query", expr(f"analyzerUdf(searchQuery, '{marketplace_id}')"))

    cap.createOrReplaceTempView("cap_view")
    normalized_cap = (
        spark.sql(f"""
            SELECT normalized_query,
                   asin,
                   kw_searches AS query_freq,
                   searches AS query_asin_impressions,
                   MAX(clicks) AS clicks,
                   MAX(adds) AS adds,
                   MAX(consumes) AS consumes,
                   MAX(purchases) AS purchases
            FROM cap_view
            GROUP BY normalized_query, asin, query_freq, query_asin_impressions, min_pos
        """)
    )

    q_cnt = normalized_cap.select("normalized_query").distinct().count()
    qa_cnt = normalized_cap.select("normalized_query", "asin").distinct().count()
    logger.info(f"latest 1-year CAP, unique query count:{q_cnt}, unique <query, asin> pair count: {qa_cnt}")

    return normalized_cap


'''
Get latest 1-year Tommy engaged <query, ASIN>
'''
def get_tommy_engaged(marketplace_name):
    # normalized_query, total_freq, asin, impressions,
    # pos, clicks, adds, purchases, consumes
    # s3://spear-tommy-top-impressions/solr8/us/normalized/20220801_20220805/match_set/token_count=1/
    s3_tommy_query_impression_asin = (
        f"s3://spear-tommy-top-impressions/solr8/{marketplace_name}/normalized/*/match_set/token_count=*/"
    )
    tommy_query_impression_asin = spark.read.parquet(s3_tommy_query_impression_asin)
    tommy_query_clicked_asin = (
        tommy_query_impression_asin.where(col("clicks") > 0)
    )

    query_freq = (
        tommy_query_clicked_asin.groupBy("normalized_query").agg(
            sum("total_freq").alias("query_freq")
        )
    )

    query_asin = (
        tommy_query_clicked_asin.groupBy("normalized_query", "asin").agg(
            sum("impressions").alias("query_asin_impressions"),
            sum("clicks").alias("clicks"),
            sum("adds").alias("adds"),
            sum("consumes").alias("consumes"),
            sum("purchases").alias("purchases")
        )
    )

    match_set = query_asin.join(query_freq, ["normalized_query"], "inner")
    return match_set


# match_set join solr_index_asin to get sp_match_set
def get_not_matched_query_set(match_set, solr_index_asin, solr_index, s3_not_matched_query_set):
    # normalized_query, asin, searches, kw_searches, cap_score
    sp_match_set = (
        match_set
            .join(solr_index_asin.hint("shuffle_hash"), ["asin"], "inner")
    )

    # Group by ASIN to get aggregated query data
    aggregated_sp_match_set = (
        sp_match_set
            .groupBy("asin")
            .agg(
                collect_list(
                    struct("normalized_query", "query_freq", "query_asin_impressions",
                       "clicks", "adds", "purchases", "consumes")
                ).alias("query_data")
            )
    )

    # Join solr_index to get ASIN title_tokens and index_tokens
    aggregated_sp_match_set_with_index = (
        aggregated_sp_match_set
            .join(solr_index.hint("shuffle_hash"), ["asin"], "inner")
    )

    # Get SP <asin, index, not_matched_query_data>
    not_matched_query_set_with_index = (
        aggregated_sp_match_set_with_index
            .withColumn(
                "not_matched_query_data",
                remove_matched_queries_UDF(col("query_data"), col("index_tokens"))
            )
            .drop("query_data")
            .where(col("not_matched_query_data") != array())
    )

    not_matched_query_set_with_index = not_matched_query_set_with_index.coalesce(2000)
    not_matched_query_set_with_index.write.mode("overwrite").parquet(s3_not_matched_query_set)


def register_solr8_tokenizer():
    spark.udf.registerJavaFunction("analyzerUdf", "com.amazon.productads.latac.udfs.Solr8LuceneAnalyzer")


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")

    working_folder = f"{s3_working_folder}{region}/{marketplace_name}/"
    s3_behavioral_match_set = f"{working_folder}behavioral_match_set/"
    s3_not_matched_query_set = f"{working_folder}solr_index_{solr_index_latest_date}/not_matched_query_set/"

    if s3_path_exists(sc, s3_not_matched_query_set):
        logger.info(f"{s3_not_matched_query_set} exists, return")
        return

    register_solr8_tokenizer()

    # Get behavioral match set as the context
    if s3_path_exists(sc, s3_behavioral_match_set):
        match_set = spark.read.parquet(s3_behavioral_match_set)
    else:
        if args.use_cap_as_context:
            match_set = construct_base_cap(args.marketplace_id, region)
        else:
            match_set = get_tommy_engaged(marketplace_name)

    # Load solr_index
    s3_solr_index = f"s3://spear-shared/dataset/solr_index/ds={solr_index_latest_date}/region={region}/"
    solr_index_asin, solr_index = load_solr_index(args.marketplace_id, s3_solr_index)

    get_not_matched_query_set(match_set, solr_index_asin, solr_index, s3_not_matched_query_set)


if __name__ == "__main__":
    main()

'''
not_matched_query_set:
root
 |-- asin: string (nullable = true)
 |-- title_tokens: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- index_tokens: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- not_matched_query_data: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- normalized_query: string (nullable = true)
 |    |    |-- query_freq: integer (nullable = true)
 |    |    |-- query_asin_impressions: integer (nullable = true)
 |    |    |-- clicks: integer (nullable = true)
 |    |    |-- adds: integer (nullable = true)
 |    |    |-- purchases: integer (nullable = true)
 |    |    |-- consumes: integer (nullable = true)
             
|0310459826|[{niv large print reference bible, 20401, 6, 2, 2, 0, 0}, 
             {niv rainbow bible, 32177, 8, 2, 0, 0, 0}, 
             {niv 1984 large print bible, 2004, 1, 1, 0, 0, 0}, 
             {niv reference bible large print thumb index, 3566, 42, 11, 2, 0, 0}, 
             {nlt holy bible, 117349, 3, 1, 0, 0, 0}, 
             {thompson chain reference bible large print, 35891, 19, 2, 0, 0, 0}]
             
query_based_missing_tokens:
+----------+---------+----------------------------------------------------------------------+
|asin      |expansion|query                                                                 |
+----------+---------+----------------------------------------------------------------------+
|0310459826|esv      |{esv thompson chain reference bible thumb index, 3460, 15, 1, 0, 0, 0}|
|0310459826|index    |{esv thompson chain reference bible thumb index, 3460, 15, 1, 0, 0, 0}|
+----------+---------+----------------------------------------------------------------------+

CAP:
root
 |-- query: string (nullable = true)
 |-- asin: string (nullable = true)
 |-- marketplace_id: long (nullable = true)
 |-- searches: long (nullable = true)
 |-- clicks: long (nullable = true)
 |-- adds: long (nullable = true)
 |-- consumes: long (nullable = true)
 |-- purchases: long (nullable = true)
 |-- qty: long (nullable = true)
 |-- amt: double (nullable = true)
 |-- price_count: long (nullable = true)
 |-- price: double (nullable = true)
 |-- kw_searches: long (nullable = true)
 |-- kw_searches_in_13: long (nullable = true)
 |-- xdf_price: double (nullable = true)
 |-- xdf_availability_class: long (nullable = true)
 |-- const: double (nullable = true)
 |-- max_searches: long (nullable = true)
 |-- alpha_const: long (nullable = true)
 |-- alpha: double (nullable = true)
 |-- laplace_hist_conv: double (nullable = true)
 |-- laplace_hist_conv_max: double (nullable = true)
 |-- rel_hist_laplace: double (nullable = true)
 |-- rel_combined_laplace: double (nullable = true)
 |-- price_mean: double (nullable = true)
 |-- price_up_bound: double (nullable = true)
 |-- price_capped: double (nullable = true)
 |-- from_consume_only_kw: integer (nullable = true)
 |-- clicks_capped: long (nullable = true)
 |-- adds_capped: long (nullable = true)
 |-- ops_normalizer: double (nullable = true)
 |-- ops_adj: double (nullable = true)
 |-- bv_hist: double (nullable = true)
 |-- bv_blend: double (nullable = true)
 |-- max_bv_blend: double (nullable = true)
 |-- bv_blend_rank: integer (nullable = true)
 |-- cap_pd: double (nullable = true)
 |-- cap_pd_round: integer (nullable = true)
 |-- cap_score: double (nullable = true)
 |-- cap_score_round: integer (nullable = true)
 |-- cap_confidence: integer (nullable = true)
 |-- date: integer (nullable = true)
'''