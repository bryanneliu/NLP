import argparse
import logging
import sys
import subprocess

from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col, desc, row_number, sum, udf
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, ArrayType, StringType
import json

from common import (
    s3_Tommy_top_impressions_prefix, s3_path_exists, format_date
)
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
    .appName("Tommy_aggregate_daily_qu_signals")
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
        help="Specify marketplaceid",
        required=True
    )

    parser.add_argument(
        "--start_date",
        type=str,
        default="2022-10-21",
        help="Start date",
        required=True
    )

    parser.add_argument(
        "--end_date",
        type=str,
        default="2022-12-24",
        help="End date",
        required=True
    )

    parser.add_argument(
        "--task_i",
        type=int,
        default=1,
        help="Specify which task to run",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


'''
Extract ProductType longest canonicalForm from Metis
Based on the analysis of 100 cases, it shows that the productType has the highest precision and coverage compared to product_sub_type, product_super_type, and product_span.
'''
def extract_product_udf(attribute):
    def extract_product_with_attribute(ner_json_str):
        if not ner_json_str:
            return ""

        ner_spans = json.loads(ner_json_str)
        ner_spans = [ner for ner in ner_spans if "start" in ner and "end" in ner]
        ner_spans = sorted(ner_spans, key=lambda x: (x["start"], -x["end"]))
        start = 0
        product_types = []
        for ner in ner_spans:
            if "start" not in ner or ner["start"] < start:
                continue

            if "attribute" in ner and ner["attribute"] == attribute:
                product_types.append(ner["canonicalForm"])
                start = ner["end"] if ner["end"] > (start + 1) else (start + 1)
                continue

            if "attributeGroup" in ner and ner["attributeGroup"] == attribute:
                product_types.append(ner["canonicalForm"])
                start = ner["end"] if ner["end"] > (start + 1) else (start + 1)
        return product_types
    return udf(extract_product_with_attribute, ArrayType(StringType()))

extract_productType_udf = extract_product_udf("productType")


'''
Extract Important non-head attribute longest canonicalForm from Metris.
Top attributeGroup:attribute are below:
:topic
target_audience
:style
:context_of_use
occasion_type
:age_range_description
:event
:target_audience_keyword
:team_name
:genre
:seasons
:author
:power_source_type
'''
def extract_attribute_udf(attribute):
    def extract_attribute(ner_json_str):
        if not ner_json_str:
            return ""

        ner_spans = json.loads(ner_json_str)
        ner_spans = [ner for ner in ner_spans if "start" in ner and "end" in ner]
        ner_spans = sorted(ner_spans, key=lambda x: (x["start"], -x["end"]))

        start = 0
        res = []
        # Extract the longest span text
        for ner in ner_spans:
            if "start" not in ner or ner["start"] < start:
                continue

            if "attribute" in ner and ner["attribute"] == attribute:
                res.append(ner["canonicalForm"])
                start = ner["end"] if ner["end"] > (start + 1) else (start + 1)
                continue

            if "attributeGroup" in ner and ner["attributeGroup"] == attribute:
                res.append(ner["canonicalForm"])
                start = ner["end"] if ner["end"] > (start + 1) else (start + 1)
        return res
    return udf(extract_attribute, ArrayType(StringType()))

'''
1. audience (text), target_audience and age_range_description (enum)
2. audience has age and gender
3. Need to be normalized
'''
extract_audience_udf = extract_attribute_udf("audience")
extract_target_audience_udf = extract_attribute_udf("target_audience")

extract_occasion_type_udf = extract_attribute_udf("occasion_type")
extract_event_udf = extract_attribute_udf("event")

extract_topic_udf = extract_attribute_udf("topic")
extract_genre_udf = extract_attribute_udf("genre")
extract_style_udf = extract_attribute_udf("style")

extract_seasons_udf = extract_attribute_udf("seasons")
extract_context_of_use_udf = extract_attribute_udf("context_of_use")

extract_team_name_udf = extract_attribute_udf("team_name")
extract_power_source_type_udf = extract_attribute_udf("power_source_type")
extract_author_udf = extract_attribute_udf("author")

'''
Extract ProductType & important non-head attribute longest canonicalForm from Metis
'''
def extract_attributes_from_metis(s3_aggregated_raw_qu_signals, s3_aggregated_extracted_qu_signals):
    qu = spark.read.parquet(s3_aggregated_raw_qu_signals)

    qu = qu.withColumn("productType", extract_productType_udf(col("metis")))

    qu = qu.withColumn("audience", extract_audience_udf(col("metis")))
    qu = qu.withColumn("target_audience", extract_target_audience_udf(col("metis")))

    qu = qu.withColumn("occasion_type", extract_occasion_type_udf(col("metis")))
    qu = qu.withColumn("event", extract_event_udf(col("metis")))

    qu = qu.withColumn("topic", extract_topic_udf(col("metis")))
    qu = qu.withColumn("genre", extract_genre_udf(col("metis")))
    qu = qu.withColumn("style", extract_style_udf(col("metis")))

    qu = qu.withColumn("seasons", extract_seasons_udf(col("metis")))
    qu = qu.withColumn("context_of_use", extract_context_of_use_udf(col("metis")))

    qu = qu.withColumn("team_name", extract_team_name_udf(col("metis")))
    qu = qu.withColumn("author", extract_author_udf(col("metis")))

    qu = qu.withColumn("power_source_type", extract_power_source_type_udf(col("metis")))

    qu = qu.coalesce(1000)
    qu.write.mode("overwrite").parquet(s3_aggregated_extracted_qu_signals)


def read_daily_qu_signals(s3_daily_qu_signals, snapshots):
    merged_qu_signals = None
    for snapshot in snapshots:
        daily_qu_signals = spark.read.parquet(s3_daily_qu_signals + snapshot)
        if not merged_qu_signals:
            merged_qu_signals = daily_qu_signals
        else:
            merged_qu_signals = merged_qu_signals.unionAll(daily_qu_signals)
    return merged_qu_signals


'''
For each daily QU signals, keep raw_query with query_freq >= 10.
Total query count between start date 20220601 and end date 20221231: 253645780 (253.65M).
1-year QU signals merge generates 349201089 (349M) raw queries. It caused out of memory issue.
To optimize the performance, split [start_date, end_date] to 2 time windows.
'''
def aggregate_daily_qu_signals_optimized(start_date, end_date, s3_daily_qu_signals, s3_aggregated_qu_signals):
    ls_output = str(subprocess.check_output(['aws', 's3', 'ls', s3_daily_qu_signals]).decode('UTF-8'))
    split_outputs = ls_output.strip().split()
    snapshots = sorted([snapshot for snapshot in split_outputs if 'PRE' not in snapshot])
    logger.info(f"snapshots[0] example: {snapshots[0]}")

    snapshots = [snapshot for snapshot in snapshots if start_date <= snapshot[:-1] <= end_date]
    window_size = len(snapshots)//2 #30->15, 31->15
    snapshots_1 = snapshots[:window_size] #0-14
    snapshots_2 = snapshots[window_size:] #15-end

    merged_qu_signals_1 = read_daily_qu_signals(s3_daily_qu_signals, snapshots_1)
    merged_qu_signals_2 = read_daily_qu_signals(s3_daily_qu_signals, snapshots_2)
    
    aggregated_qu_signals_1 = aggregate_merged_qu_signals(merged_qu_signals_1)
    aggregated_qu_signals_2 = aggregate_merged_qu_signals(merged_qu_signals_2)
    merged_aggregated_qu_signals = aggregated_qu_signals_1.unionAll(aggregated_qu_signals_2)
    aggregated_qu_signals = aggregate_merged_qu_signals(merged_aggregated_qu_signals)

    logger.info(f"Daily QU signals between start date {start_date} and end date {end_date} are aggregated.")

    aggregated_qu_signals = aggregated_qu_signals.coalesce(1000)
    aggregated_qu_signals.write.mode("overwrite").parquet(s3_aggregated_qu_signals)

    logger.info(f"Writing aggregated QU signals between start date {start_date} and end date {end_date} to S3 is done.")


def aggregate_merged_qu_signals(merged_qu_signals):
    # Aggregate query_freq for each raw_query
    query_freq = (
        merged_qu_signals
            .select("raw_query", "query_freq")
            .groupBy("raw_query").agg(
                sum("query_freq").alias("query_freq")
            )
    )
    logger.info(f"query_freq: {query_freq.count()}")

    # Keep the latest QU sigals for each raw_query
    w = Window.partitionBy("raw_query").orderBy(desc("partition_date"))
    query_clustered_sorted = merged_qu_signals.withColumn("rank", row_number().over(w)).drop("rank")
    latest_qu_signals = query_clustered_sorted.where(col("rank") == 1).drop("query_freq")
    logger.info(f"latest_qu_signals: {latest_qu_signals.count()}")

    # Join latest_qu_signals with query_freq
    aggregated_qu_signals = latest_qu_signals.join(query_freq, ["raw_query"], "inner")

    return aggregated_qu_signals


'''
Specify start_date and end_date, Aggregate daily QU signals from start_date to end_date:
1. Cluster by raw_query: Sum(query_freq)
2. Cluster by raw_query, sort by partition_date: Keep the top 1 (the most recent) metis and pt_scores

Performance Optimization:
snapshots = [snapshot for snapshot in snapshots if start_date <= snapshot[:-1] <= end_date]
merged_qu_signals = spark.read.parquet([s3_daily_qu_signals + snapshot for snapshot in snapshots])
.withColumn("query_freq", sum("query_freq").over(Window.partitionBy("raw_query")))
'''
def aggregate_daily_qu_signals(start_date, end_date, s3_daily_qu_signals, s3_aggregated_qu_signals):
    # Merge daily_qu_signals from date_range_start to date_range_end
    ls_output = str(subprocess.check_output(['aws', 's3', 'ls', s3_daily_qu_signals]).decode('UTF-8'))
    split_outputs = ls_output.strip().split()
    snapshots = sorted([snapshot for snapshot in split_outputs if 'PRE' not in snapshot])
    logger.info(f"snapshots[0] example: {snapshots[0]}")
    
    merged_qu_signals = None
    for date_str in snapshots:
        date_str = date_str[:-1]
        if (date_str >= start_date) and (date_str <= end_date):
            logger.info(f"Include dates: {date_str}")
            daily_qu_signals = spark.read.parquet(s3_daily_qu_signals + date_str)
            if not merged_qu_signals:
                merged_qu_signals = daily_qu_signals
            else:
                merged_qu_signals = merged_qu_signals.union(daily_qu_signals)

    # Aggregate query_freq for each raw_query
    query_freq = (
        merged_qu_signals
            .select("raw_query", "query_freq")
            .groupBy("raw_query").agg(
                sum("query_freq").alias("query_freq")
            )
    )
    logger.info(f"query_freq: {query_freq.count()}")

    # Keep the latest QU signals for each raw_query
    w = Window.partitionBy("raw_query").orderBy(desc("partition_date"))
    query_clustered_sorted = merged_qu_signals.withColumn("rank", row_number().over(w)).drop("rank")
    latest_qu_signals = query_clustered_sorted.where(col("rank") == 1).drop("query_freq")
    logger.info(f"latest_qu_signals: {latest_qu_signals.count()}")
        
    # Join latest_qu_signals with query_freq
    aggregated_qu_signals = latest_qu_signals.join(query_freq, ["raw_query"], "inner")

    # Output
    aggregated_qu_signals_count = aggregated_qu_signals.count()    
    logger.info(f"Total query count between start date {start_date} and end date {end_date}: {aggregated_qu_signals_count}")

    aggregated_qu_signals = aggregated_qu_signals.coalesce(500)
    aggregated_qu_signals.write.mode("overwrite").parquet(s3_aggregated_qu_signals)



def main():
    # Initialization
    args = process_args()

    if args.task_i != 4:
        return

    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    
    s3_qu_signals_root_folder = f"{s3_Tommy_top_impressions_prefix}{marketplace_name}/qu/"
    s3_daily_qu_signals = f"{s3_qu_signals_root_folder}daily/"
    
    start_date = format_date(args.start_date, "%Y%m%d")
    end_date = format_date(args.end_date, "%Y%m%d")
    
    # Aggregate daily qu signals from start_date to end_date
    s3_aggregated_qu_signals = f"{s3_qu_signals_root_folder}aggregated/{start_date}_{end_date}/raw/"
    aggregate_daily_qu_signals_optimized(start_date, end_date, s3_daily_qu_signals, s3_aggregated_qu_signals)

    # Extract productType and important attributes from metis from start_date to end_date
    s3_aggregated_extracted_qu_signals = f"{s3_qu_signals_root_folder}aggregated/{start_date}_{end_date}/extracted/"
    extract_attributes_from_metis(s3_aggregated_qu_signals, s3_aggregated_extracted_qu_signals)


if __name__ == "__main__":
    main()


