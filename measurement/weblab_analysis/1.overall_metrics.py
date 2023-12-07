import argparse
import logging
import sys

import matplotlib.pyplot as plt
import io
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
import pyspark.sql.functions as F

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
    .appName("weblab_analysis_overall_metrics")
    .enableHiveSupport()
    .getOrCreate()
)



def calculate_overall_metrics(args, marketplace):
    s3_output = f"{cm.s3_weblab_analysis}{args.Solr_or_Horus.lower()}/{args.weblab_name}/{marketplace}/overall/"

    raw_data = spark.sql(f"""
        SELECT *
        FROM {args.feed_glue_table}
        WHERE weblab_name = '{args.weblab_name}'
            AND feed_date >= '{args.start_date}'
            AND feed_date <= '{args.end_date}'
            AND marketplace_id = {args.marketplace_id}
    """)

    overall_metrics_treatment = raw_impression_data_aggr(raw_data, ['treatment', 'marketplace_id'])
    logger.info("Treatment Level Overall metrics:")
    overall_metrics_treatment.sort("marketplace_id", "treatment").show(100, False)

    overall_metrics = raw_impression_data_aggr(raw_data, ['feed_date', 'treatment', 'marketplace_id']).cache()
    logger.info("Feed_Date Level Overall metrics:")
    overall_metrics.sort("feed_date", "treatment").show(100, False)
    overall_metrics.repartition(1).write.mode("overwrite").option("header","true").csv(s3_output + "summary_by_date")

    return overall_metrics


def plot_metric(df, metric, ax, baseline, treatment, color="blue"):
    df = df.sort_values("feed_date", axis=0)

    group = df[df["treatment"] == treatment][["feed_date", metric]].merge(
        df[df["treatment"] == baseline][["feed_date", metric]], on="feed_date", how="inner"
    )
    group["diff"] = group[f"{metric}_x"] - group[f"{metric}_y"]
    group.plot(kind="scatter", x="feed_date", y="diff", color=color, s=32, ax=ax)

    ax.axhline(y=0, color='r', linewidth=3)
    ax.set_title(f"{metric} diff with {baseline}", fontsize=16, fontweight="bold")
    ax.tick_params(axis='x', labelrotation=45)


# https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteAccessPermissionsReqd.html
def generate_report(args, region, marketplace, overall_metrics, baseline, treatment):
    s3 = boto3.resource('s3')
    s3_key_prefix = f"weblab_analysis/{args.Solr_or_Horus.lower()}/{args.weblab_name}/{marketplace}/overall/"

    metrics = [
        "sum_rev",
        "impressions",
        "clicks",
        "sum_ops",
        "avg_p_irrel",
        "avg_ectr",
        "actr",
        "avg_cpc",
        "avg_ecvr",
        "avg_roas"
    ]

    fig, axes = plt.subplots(4, 2, sharex=True, sharey=False, figsize=(15, 15))
    fig.tight_layout(pad=5)
    fig.suptitle(f"{args.weblab_name} {region} {marketplace} weblab analysis - {treatment}", fontsize=24, fontweight="bold")
    fig.subplots_adjust(top=0.9)

    df = overall_metrics.toPandas()
    for metric, ax in zip(metrics, axes.reshape(-1)):
        plot_metric(df, metric, ax, baseline, treatment)

    img_data = io.BytesIO()
    fig.savefig(img_data, format='png', bbox_inches='tight')
    img_data.seek(0)
    object = s3.Object(cm.s3_bucket, f"{s3_key_prefix}{treatment}_vs_{baseline}.png")

    # upload the image to s3
    object.put(Body=img_data.read())

def raw_impression_data_aggr(df, group_by_cols):
    df = (
        df.withColumn('ops', F.aggregate("sale_price", F.lit(0.0), lambda acc, x: acc + x))
            .groupby(*group_by_cols)
            .agg(
                F.mean(F.col('click')).alias('actr'),
                F.mean(F.col('cpc')).alias('avg_cpc'),
                F.mean(F.col('p_irrel')).alias('avg_p_irrel'),
                F.mean(F.col('revenue')).alias('avg_rev'),
                F.mean(F.col('ECTR')).alias('avg_ectr'),
                F.mean(F.col('ECVR')).alias('avg_ecvr'),
                (F.round(F.sum('ops')/F.sum('revenue'), 6)).alias('avg_roas'),
                F.countDistinct('asin', 'advertiser_id').alias('asin_advid_count'),
                F.countDistinct('http_request_id').alias('total_http_request'),
                F.countDistinct('advertiser_id').alias('total_advertiser'),
                F.sum('revenue').alias('sum_rev'),
                F.sum('ops').alias('sum_ops'),
                F.count('*').alias('impressions'),
                F.sum('click').alias('clicks'),
                F.round(F.count('*')*1.0/F.countDistinct("http_request_id"), 4).alias("ad_density"))
        )
    return df

'''
Calculate treatment and baseline diff on sum_revenue, impressions, clicks, ops, p_irrel, ectr, actr, cpc, ecvr, avg_roas
'''
def main():
    # Initialization
    args = cm.process_args()

    region = get_region_name(args.marketplace_id).upper()
    marketplace = get_marketplace_name(args.marketplace_id).upper()

    overall_metrics = calculate_overall_metrics(args, marketplace)

    baseline = "T1" if args.Solr_or_Horus.lower() == "solr" else "C"
    treatments = ["T2", "T3"] if args.Solr_or_Horus.lower() == "solr" else ["T1", "T2", "T3", "T4", "T5"]

    for treatment in treatments:
        generate_report(args, region, marketplace, overall_metrics, baseline, treatment)


if __name__ == "__main__":
    main()
