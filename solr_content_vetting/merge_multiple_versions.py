import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, split, size, max, avg, sum, count, min, collect_set, concat_ws, lit, when, row_number
import pyspark.sql.functions as F

from common import (
    s3_working_folder, format_date, s3_path_exists, remove_bad_expansion_UDF
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
    .appName("merge_multiple_version_and_calculate_expansion_distribution")
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
        default="2022-12-19",
        help="Specify solr_index ds",
        required=True
    )

    parser.add_argument(
        "--head_asin_expansion_count_threshold",
        type=int,
        default=200,
        help="Use asin expansion count to define HEAD ASINs",
        required=True
    )

    parser.add_argument(
        "--head_asin_capped_token_count",
        type=int,
        default=50,
        help="Keep max token count for HEAD ASINs",
        required=True
    )

    parser.add_argument(
        "--non_head_asin_capped_token_count",
        type=int,
        default=30,
        help="Keep max token count for non-HEAD ASINs",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


def calculate_token_distribution(data, condition):
    data_with_condition = data.where(condition)
    data_count = data.count()
    data_with_condition_count = data_with_condition.count()

    logger.info(f"{condition}: {data_with_condition_count}, {round(data_with_condition_count / data_count, 4)}")


def calculate_output_distribution(asin_expansion_merged, solr_index_asin_count):
    data = asin_expansion_merged.withColumn("addedTokenCount", size(split(col("tokens"), " ")))
    data.cache()

    calculate_token_distribution(data, col("addedTokenCount") == 1)
    calculate_token_distribution(data, (col("addedTokenCount") > 1) & (col("addedTokenCount") <= 5))
    calculate_token_distribution(data, (col("addedTokenCount") > 5) & (col("addedTokenCount") <= 10))
    calculate_token_distribution(data, (col("addedTokenCount") > 10) & (col("addedTokenCount") <= 20))
    calculate_token_distribution(data, (col("addedTokenCount") > 20) & (col("addedTokenCount") < 30))
    calculate_token_distribution(data, col("addedTokenCount") == 30)
    calculate_token_distribution(data, (col("addedTokenCount") > 30) & (col("addedTokenCount") < 50))
    calculate_token_distribution(data, col("addedTokenCount") == 50)
    calculate_token_distribution(data, col("addedTokenCount") > 50)
    calculate_token_distribution(data, col("addedTokenCount") > 100)

    addedTokenCount = data.agg(
        count("asin").alias("covered_asin_count"),
        F.round(count("asin") / solr_index_asin_count, 4).alias("covered_asin_coverage"),
        max("addedTokenCount").alias("max_added_tokens"),
        avg("addedTokenCount").alias("avg_added_tokens_for_covered_asins"),
        sum("addedTokenCount").alias("total_added_tokens")
    )
    addedTokenCount.show(5, False)


def calculate_solr_index_asin_count(region, marketplace_id, solr_index_latest_date):
    s3_solr_index = f"s3://spear-shared/dataset/solr_index/ds={solr_index_latest_date}/region={region}/"
    solr_index = spark.read.parquet(s3_solr_index)

    solr_index_asin = solr_index.where(col("marketplaceid") == marketplace_id).select("asin")
    solr_index_asin_count = solr_index_asin.count()
    logger.info(f"solr_index unique ASIN count: {solr_index_asin_count}\n")

    return solr_index_asin_count


def generate_statistics(s3_asin_expansion, solr_index_asin_count):
    logger.info("#####################################################################")
    logger.info("Calculate output token distribution")
    logger.info(f"For data set: {s3_asin_expansion}")
    logger.info("#####################################################################")

    asin_expansion = spark.read.parquet(s3_asin_expansion)
    calculate_output_distribution(asin_expansion, solr_index_asin_count)


# Percentile is in [0,1], where 0 represents the lowest rank, it's in descending order
# query top 50 percentile: Thresholds(cap_score:0.0259, distinct_query_count:3, total_clicks:12)
# overlap_query_threshold=0.5, overlap_tc_threshold=1, query_threshold=0.3
# overlap_query_threshold=0.5, overlap_tc_threshold=1, query_threshold=-1
def generate_top_expansion(args, merged_asin_expansion, overlap_query_threshold, overlap_tc_threshold, query_threshold, s3_merged_top_expansion, solr_index_asin_count):
    logger.info(f"overlap_query_threshold: {overlap_query_threshold}")
    logger.info(f"overlap_tc_threshold: {overlap_tc_threshold}")
    logger.info(f"query_threshold: {query_threshold}")

    head_asin_capped_token_count = args.head_asin_capped_token_count # 50
    non_head_asin_capped_token_count = args.non_head_asin_capped_token_count # 30
    logger.info(f"head_asin_capped_token_count: {head_asin_capped_token_count}")
    logger.info(f"non_head_asin_capped_token_count: {non_head_asin_capped_token_count}")

    overlap_condition = (
            (col("distinct_query_count_percentile") <= overlap_query_threshold) &
            (col("total_clicks_percentile") <= overlap_query_threshold) &
            (col("cap_score_percentile") <= overlap_query_threshold) &
            (col("distinct_tc_count_percentile") <= overlap_tc_threshold)
    )

    top_query_expansion_condition = (
            (col("distinct_query_count_percentile") <= query_threshold) &
            (col("total_clicks_percentile") <= query_threshold) &
            (col("cap_score_percentile") <= query_threshold)
    )

    # Get Top Expansions
    top_expansion = (
        merged_asin_expansion
            .where(overlap_condition | top_query_expansion_condition)
            .withColumn("is_filtered", remove_bad_expansion_UDF(col("asin"), col("expansion")))
            .where(col("is_filtered") == 0)
    )

    asin_expansion_count = (
        top_expansion
            .groupBy("asin").agg(collect_set("expansion").alias("expansion_set"))
            .withColumn("expansion_count", size(col("expansion_set")))
            .drop("expansion_set")
    )

    top_expansion = top_expansion.join(asin_expansion_count, ["asin"], "inner")

    # Truncate top expansions
    # Without truncation, head ASINs can generate as high as 6000 expansions (for example, B01M4MCUAF)
    # order_expansion = Window().partitionBy("asin").orderBy(col("distinct_query_count").desc(), col("total_clicks").desc())
    order_expansion = Window().partitionBy("asin").orderBy(col("total_clicks").desc())
    truncated_top_expansion = (
        top_expansion
            .withColumn("rn", row_number().over(order_expansion))   # 200
            .where(col("rn") <= when(col("expansion_count") >= args.head_asin_expansion_count_threshold, head_asin_capped_token_count).otherwise(non_head_asin_capped_token_count))
    )
    truncated_top_expansion.show(10, False)

    # Output
    output = (
        truncated_top_expansion
            .select("asin", "expansion")
            .groupBy("asin").agg(collect_set("expansion").alias("expansion_set"))
            .withColumn("tokens", concat_ws(" ", col("expansion_set")))
            .select("asin", lit(args.marketplace_id).alias("marketplaceId"), "tokens")
    )
    output = output.coalesce(10)

    s3_output = f"{s3_merged_top_expansion}overlap_query_{str(overlap_query_threshold*100)}_tc_{str(overlap_tc_threshold*100)}_topQuery_{str(query_threshold*100)}/"
    output.write.mode("overwrite").parquet(s3_output)

    generate_statistics(s3_output, solr_index_asin_count)


def main():
    # Initialization
    args = process_args()

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()
    working_folder = f"{s3_working_folder}{region}/{marketplace_name}/"

    # Get latest solr index asin count
    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")
    solr_index_asin_count = calculate_solr_index_asin_count(region, args.marketplace_id, solr_index_latest_date)

    # Input
    s3_asin_expansion_rank = f"{working_folder}*/asin_expansion_rank/"
    # Output
    s3_merged_top_expansion = f"{working_folder}merged_top_expansion/"

    asin_expansion_rank = spark.read.parquet(s3_asin_expansion_rank)

    fill_count = (
        asin_expansion_rank
            .fillna(0) # fill null count with 0
            .groupBy("asin", "expansion").agg(
                max("distinct_query_count").alias("distinct_query_count"),
                max("total_clicks").alias("total_clicks"),
                max("cap_score").alias("cap_score"),
                max("distinct_tc_count").alias("distinct_tc_count")
            )
    )

    fill_percentile = (
        asin_expansion_rank
            .fillna(2) # fill null percentile with 2
            .groupBy("asin", "expansion").agg(
                min("distinct_query_count_percentile").alias("distinct_query_count_percentile"),
                min("total_clicks_percentile").alias("total_clicks_percentile"),
                min("cap_score_percentile").alias("cap_score_percentile"),
                min("distinct_tc_count_percentile").alias("distinct_tc_count_percentile")
            )
    )

    merged_asin_expansion = fill_count.join(fill_percentile, ["asin", "expansion"], "inner")

    # Overlap (top 50% query + all tc) + Top query expansion (top 30% query)
    overlap_query_threshold, overlap_tc_threshold, query_threshold = 0.5, 1, 0.3
    generate_top_expansion(args, merged_asin_expansion, overlap_query_threshold, overlap_tc_threshold, query_threshold, s3_merged_top_expansion, solr_index_asin_count)

    # Overlap (top 50% query + all tc)
    overlap_query_threshold, overlap_tc_threshold, query_threshold = 0.5, 1, -1
    generate_top_expansion(args, merged_asin_expansion, overlap_query_threshold, overlap_tc_threshold, query_threshold, s3_merged_top_expansion, solr_index_asin_count)

    # Overlap (top 50% query + top 40% tc) + Top query expansion (top 25% query)
    overlap_query_threshold, overlap_tc_threshold, query_threshold = 0.5, 0.4, 0.25
    generate_top_expansion(args, merged_asin_expansion, overlap_query_threshold, overlap_tc_threshold, query_threshold, s3_merged_top_expansion, solr_index_asin_count)

    # Overlap (top 50% query + top 40% tc)
    overlap_query_threshold, overlap_tc_threshold, query_threshold = 0.5, 0.4, -1
    generate_top_expansion(args, merged_asin_expansion, overlap_query_threshold, overlap_tc_threshold, query_threshold, s3_merged_top_expansion, solr_index_asin_count)


if __name__ == "__main__":
    main()


'''
merged_asin_expansion:
+----------+---------------+--------------------+------------+------------------+-------------------------------+-----------------------+--------------------+-----------------+----------------------------+
|asin      |expansion      |distinct_query_count|total_clicks|cap_score         |distinct_query_count_percentile|total_clicks_percentile|cap_score_percentile|distinct_tc_count|distinct_tc_count_percentile|
+----------+---------------+--------------------+------------+------------------+-------------------------------+-----------------------+--------------------+-----------------+----------------------------+
|0008276331|hardcover      |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|006026683X|want           |4                   |12          |1.5084            |0.28971462059780645            |0.48305949212521065    |0.11537755065418981 |null             |null                        |
|0061246476|prairie        |4                   |40          |0.208             |0.28971462059780645            |0.15788973787837546    |0.23681439742843813 |null             |null                        |
|0062954547|behavior       |3                   |9           |0.0263            |0.39089015315851433            |0.5973140260957329     |0.5023861507268769  |null             |null                        |
|0140502343|going          |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0142410578|popular        |null                |null        |null              |null                           |null                   |null                |3                |0.19945749205244911         |
|0143122355|red            |3                   |7           |1.4848999999999999|0.39089015315851433            |0.7008232979994895     |0.12731252822572656 |null             |null                        |
|0241455154|dinner         |4                   |15          |0.0505            |0.28971462059780645            |0.4041479585301808     |0.35058899005662686 |null             |null                        |
|0244806748|19171922       |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0244806748|4again         |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0244806748|berna          |null                |null        |null              |null                           |null                   |null                |2                |0.26775857120591934         |
|0244806748|campucia       |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0244806748|cybernetically |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0244806748|deflorarti     |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0244806748|delllesploslone|null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0244806748|dichiudere     |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0244806748|disegnarcele   |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0244806748|divincolava    |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0244806748|dorsorugoso    |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
|0244806748|dÃ¡me           |null                |null        |null              |null                           |null                   |null                |1                |0.4071322963655497          |
+----------+---------------+--------------------+------------+------------------+-------------------------------+-----------------------+--------------------+-----------------+----------------------------+
only showing top 20 rows
'''

