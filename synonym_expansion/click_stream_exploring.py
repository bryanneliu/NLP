import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, explode, collect_set, countDistinct, sum, percent_rank, max
from pyspark.sql.window import Window

from spark_io import *
from udf import *

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
    .appName("click-stream-based-token-expansion")
    .enableHiveSupport()
    .getOrCreate()
)

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
        "--top_percentile",
        type=float,
        default=0.7,
        help="Select <asin, token> in top percentile",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args

marketplace_mapping = {
    1: ("NA", "us"),
    7: ("NA", "ca"),
    3: ("EU", "uk")
}

def load_solr_index(args, region):
    s3_solr_index = f"s3://spear-shared/dataset/solr_index/ds={args.solr_index_latest_ds}/region={region}/"
    solr_index = spark.read.parquet(s3_solr_index)
    solr_index = solr_index.where(col("marketplaceid") == int(args.marketplace_id)).select("asin", "index_tokens")
    solr_index_asin = solr_index.select("asin")
    solr_index_asin.cache()

    logging.info(f"solr_index unique ASIN count: {solr_index_asin.count()}")
    return solr_index, solr_index_asin


# The single match set is aggregated by <normalized_query, asin, sum(clicks), sum(purchases)>
# Combined match set is aggregated by <normalized_query, asin>

'''
Single match set generation:
1. From searchdata.tommy_asin, A = <raw_query, count("request_id") as freq, marketplace_id>
2. Use prod solr5 analyzer to normalize raw_query, B = <normalized_query, token_count, freq>
3. C = <normalized_query, sum("freq") as total_freq
4. From searchdata.tommy_asin, D = <raw_query, asin, sum("clicks") as clicks, sum("purchases") as purchases>
5. D inner join C to get <normalized_query, token_count, total_freq, asin, clicks, purchases>

Combine all match sets:
'''
def load_match_set(args, org):
    s3_match_set = f"s3://synonym-expansion-experiments/asin_level_synonym_expansion/{org}/*/match_set/token_count=*/"
    match_set = spark.read.parquet(s3_match_set)

    top_match_set = match_set.where(col("clicks") > 0)

    top_match_set = top_match_set.groupBy("normalized_query", "asin").agg(
        sum("total_freq").alias("query_freq"),  # query freq
        sum("impressions").alias("qa_impressions"),  # <query, asin> freq
        sum("clicks").alias("clicks"),
        sum("adds").alias("adds"),
        sum("purchases").alias("purchases"),
        sum("consumes").alias("consumes")
    )

    return top_match_set


def main():
    # Initialization
    args = process_args()

    if args.marketplace_id not in marketplace_mapping:
        raise Exception(f"marketplace_id {args.marketplace_id} is not supported")

    region = marketplace_mapping[args.marketplace_id][0]
    org = marketplace_mapping[args.marketplace_id][1]

    # Define output
    s3_output = f"s3://synonym-expansion-experiments/{region}/{org}/click-stream/"

    ################################
    # Generate <ASIN, token, debug>
    ################################
    # Load latest solr_index
    solr_index, solr_index_asin = load_solr_index(args, region)

    # Load match_set
    top_match_set = load_match_set(args, org)

    # Match set join solr_index
    sp_match_set_with_asin = top_match_set.join(solr_index_asin.hint("shuffle_hash"), ["asin"], "inner")
    sp_match_set_with_index = sp_match_set_with_asin.join(solr_index.hint("shuffle_hash"), ["asin"], "inner")

    # Expand ASIN index tokens with not matched query tokens
    asin_expansion = (
        sp_match_set_with_index
            .withColumn("expansion_tokens", getNotMatchedQueryTokensUDF(col("normalized_query"), col("index_tokens")))
            .where(col("expansion_tokens") != array())
    )

    asin_expansion = (
        asin_expansion
        .select("asin", "normalized_query", "query_freq", "clicks", "qa_impressions",
                "adds", "purchases", "consumes", explode("expansion_tokens").alias("token"))
    )

    asin_token = asin_expansion.groupBy("asin", "token").agg(
        collect_set("normalized_query").alias("distinct_query_set"),
        countDistinct("normalized_query").alias("distinct_query_count"),
        sum("query_freq").alias("total_query_freq"),
        sum("qa_impressions").alias("total_qa_impressions"),
        sum("clicks").alias("total_clicks"),
        sum("adds").alias("total_adds"),
        sum("purchases").alias("total_purchases"),
        sum("consumes").alias("total_consumes"),
        max("clicks").alias("max_clicks")
    )
    asin_token.show(30, False)

    # Write <asin, token> debug info to s3
    asin_token.repartition(1000).write.mode("overwrite").parquet(s3_output + "debug/")

    ####################
    # Select ASIN Token
    ####################

    '''
    ASIN token score function tuning:
    +-------------+-------------+--------------------+------------+
    |total_clicks |max_clicks   |distinct_query_count|support     |
    +-------------+-------------+--------------------+------------+
    | 11          | 1           | 11                 | Weak       |
    +-------------+-------------+--------------------+------------+
    | 121         | 120         | 2                  | Weak       |
    +-------------+-------------+--------------------+-------------

    Strong support - Different queries & at least 2 of them have good clicks
    '''

    asin_token_filtered = (
        asin_token
            .select("asin", "token", "distinct_query_count", "total_clicks", "max_clicks",
                    "total_adds", "total_purchases", "total_consumes")
            .where((col("distinct_query_count") > 1)
                   & (col("max_clicks") > 2)
                   & ((col("total_clicks") - col("max_clicks")) >= col("distinct_query_count"))
                   # at least 2 queries with >= 2 clicks
                   )
    )

    # withColumn("cap_score", col("total_clicks") * 0.0007 + col("total_adds") * 0.02 + col("total_purchases") * 1.4 + col("total_consumes") * 1.4)
    asin_token_score = (
        asin_token_filtered
            .withColumn("score", col("distinct_query_count") * 0.1 + col("total_clicks") * 0.01 + col("total_adds") * 0.02 + col("total_purchases") * 1.4 + col("total_consumes") * 1.4)
    ).cache()

    logging.info(f"ASIN token with max_clicks > 2 & distinct_query_count > 1 & at least 2 queries with >= 2 clicks: {asin_token_score.count()}")

    order_score = Window.orderBy(col("cap_score"))
    asin_token_percentile = (
        asin_token_score
            .select("asin", "token", "total_clicks", "max_clicks", "distinct_query_count", "cap_score")
            .withColumn("percentile", percent_rank().over(order_score))
    ).cache()

    asin_token_selection = asin_token_percentile.where(col("percentile") >= args.top_percentile).cache()

    logging.info(f"Selected asin token count: {asin_token_selection.count()}")
    asin_token_selection.sort("distinct_query_count", ascending=True).show(100, False)

    # Write selected asin token into s3
    asin_token_selection.repartition(1000).write.mode("overwrite").parquet(s3_output + "asin_token_selected/")

if __name__ == "__main__":
    main()
