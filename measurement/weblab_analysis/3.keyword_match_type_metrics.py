import argparse
import logging
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, count, avg, round, when, sum, lit, coalesce
from pyspark.sql.window import Window

import itertools
import matplotlib.pyplot as plt
import io
import boto3

from datetime import date, timedelta, datetime

import common as cm
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
    .appName("keyword_match_type")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext


def get_keyword_match_type_distribution(data):
    distribution = aggregate_by_columns(data, ["treatment", "keyword_match_type"])
    logger.info("keyword_match_type_distribution:")
    (
        distribution
            .withColumn('rev_share', F.col('sum_revenue')/F.sum('sum_revenue').over(Window.partitionBy("treatment")))\
            .withColumn('impr_share', F.col('sum_impr')/F.sum('sum_impr').over(Window.partitionBy("treatment")))
            .orderBy(F.col('treatment').asc(), F.col('type_rank').asc()).drop('type_rank')
            .show(200, False)
    ) 

    distribution = aggregate_by_columns(data, ["treatment", "keyword_match_type", "has_nkw"])
    logger.info("keyword_match_type_with_nkw_distribution:")
    (
        distribution
            .withColumn('rev_share', F.col('sum_revenue')/F.sum('sum_revenue').over(Window.partitionBy("treatment")))\
            .withColumn('impr_share', F.col('sum_impr')/F.sum('sum_impr').over(Window.partitionBy("treatment")))
            .orderBy(F.col('treatment').asc(), F.col('type_rank').asc(), F.col("has_nkw").asc()).drop('type_rank')
            .show(200, False)
    ) 

    distribution = aggregate_by_columns(data, ["feed_date", "treatment", "keyword_match_type", "has_nkw"])
    return distribution

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
    ).withColumn('type_rank', 
        F.when(F.col('keyword_match_type')=='exact', 1)
        .when(F.col('keyword_match_type')=='phrase', 2)
        .when(F.col('keyword_match_type')=='broad', 3)
        .when(F.col('keyword_match_type')=='close-match', 4)
        .when(F.col('keyword_match_type')=='loose-match', 5)
        .when(F.col('keyword_match_type')=='substitutes', 6).otherwise(7))
    return df

def fig_to_s3(fig, bucket, path):
    s3 = boto3.resource('s3')
    img_data = io.BytesIO()
    fig.savefig(img_data, format='png', bbox_inches='tight')
    img_data.seek(0)
    object = s3.Object(bucket, path)

    # upload the image to s3
    object.put(Body=img_data.read())


def plot_metric(df, metric, ax, baseline, treatment, color="blue", title=None):
    df = df.sort_values("feed_date", axis=0)

    group = df[df["treatment"] == treatment][["feed_date", metric]].merge(
        df[df["treatment"] == baseline][["feed_date", metric]], on="feed_date", how="inner"
    )
    group["diff"] = group[f"{metric}_x"] - group[f"{metric}_y"]
    group.plot(kind="scatter", x="feed_date", y="diff", color=color, s=32, ax=ax)

    ax.axhline(y=0, color='r', linewidth=3)
    if not title:
        ax.set_title(f"{metric} diff with {baseline}", fontsize=16, fontweight="bold")
    else:
        ax.set_title(title)
    ax.tick_params(axis='x', labelrotation=45)


def generate_keyword_match_type_distribution_report(args, marketplace, overall_metrics, baseline, treatment):
    s3_key_prefix = f"weblab_analysis/{args.Solr_or_Horus.lower()}/{args.weblab_name}/{marketplace}/keyword_match_type/"

    metrics = [
        "sum_revenue",
        "click_count",
        "avg_p_irrel",
        "avg_cpc",
    ]

    match_types = [
        "broad",
        "phrase",
        "loose-match",
        "close-match",
        "exact",
    ]

    df = overall_metrics.toPandas()

    fig = plt.figure(constrained_layout=True, figsize=(30, 30))
    metric_figure_columns = fig.subfigures(1, 4)
    fig.suptitle(f"{args.weblab_name} {marketplace} weblab analysis - {treatment}", fontsize=24, fontweight="bold")

    for metric, subfigure in zip(metrics, metric_figure_columns):
        subfigure.suptitle(f"{metric} diff with {baseline}", fontsize=16, fontweight="bold")
        axes = subfigure.subplots(10, 1, sharex=True)

        for (match_type, nkw), ax in zip(itertools.product(match_types, [0, 1]), axes.reshape(-1)):
            plot_metric(
                df[(df["keyword_match_type"] == match_type) & (df["has_nkw"] == nkw)],
                metric,
                ax,
                baseline,
                treatment,
                title=f"{match_type} {'nkw' if nkw == 1 else ''}",
            )

    fig_to_s3(fig, cm.s3_bucket, f"{s3_key_prefix}{treatment}_vs_{baseline}.png")


'''
Calculate treatment and baseline diff on different keyword match types with and without nkw
'''
def main():
    # Initialization
    args = cm.process_args()

    marketplace = get_marketplace_name(args.marketplace_id).upper()

    data = cm.load_impressed_ad_details(spark, args)

    keyword_match_distribution = get_keyword_match_type_distribution(data)

    baseline = "T1" if args.Solr_or_Horus.lower() == "solr" else "C"
    treatments = ["T2", "T3"] if args.Solr_or_Horus.lower() == "solr" else ["T1", "T2", "T3", "T4", "T5"]

    for treatment in treatments:
        generate_keyword_match_type_distribution_report(args, marketplace, keyword_match_distribution, baseline, treatment)


if __name__ == "__main__":
    main()
