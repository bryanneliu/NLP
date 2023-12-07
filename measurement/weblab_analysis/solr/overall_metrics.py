import argparse
import logging
import matplotlib.pyplot as plt
import io
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

from constants import *

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
        default="2022-12-02",
        help="Specify weblab start date to analyze on",
        required=True
    )

    parser.add_argument(
        "--end_date",
        type=str,
        default=1,
        help="Specify weblab end date to analyze on",
        required=True
    )

    parser.add_argument(
        "--weblab_name",
        type=str,
        default="SPONSORED_PRODUCTS_546406",
        help="Use as filtering",
        required=True
    )

    parser.add_argument(
        "--feature_name",
        type=str,
        default="Solr8",
        help="Use as output folder",
        required=True
    )

    parser.add_argument(
        "--feed_glue_table",
        type=str,
        default="spear_lbryanne.spear_weblab_logging",
        help="Use FEED to pull weblab data into this glue table, query it for weblab impression data",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


def calculate_overall_metrics(args, region_org):
    region, org = region_org[0], region_org[1]
    s3_output = f"{s3_weblab_analysis}solr/{args.feature_name}/{org}/overall/"

    overall_metrics = spark.sql(f"""
        SELECT feed_date,
            treatment,
            marketplace_id,
            sum(revenue) as sum_rev,
            count(*)  as impressions,
            sum(click)  as clicks,
            avg(p_irrel) as avg_p_irrel,
            avg(ectr) as avg_ectr,
            avg(ecvr) as avg_ecvr,
            avg(cpc) as avg_cpc,
            count(distinct http_request_id) as total_http_request,
            count(distinct session_id)  as total_session,
            count(distinct advertiser_id) as total_advertiser
        FROM {args.feed_glue_table}
        WHERE page_type = 'Search'
            AND weblab_name = '{args.weblab_name}'
            AND feed_date >= '{args.start_date}'
            AND feed_date <= '{args.end_date}'
            AND marketplace_id = {args.marketplace_id}
        GROUP BY 1,
            2,
            3
    """)

    overall_metrics = (overall_metrics
                       .select("marketplace_id", "feed_date", "treatment", "sum_rev",
                               "impressions", "clicks", "avg_p_irrel", "avg_ectr", "avg_ecvr", "avg_cpc")
                       .withColumn("actr", round(col("clicks") / col("impressions"), 6))
                       ).cache()

    logger.info("Overall Metrics:")
    overall_metrics.sort("feed_date", "treatment", "sum_rev").show(100, False)

    overall_metrics.repartition(1).write.mode("overwrite").option("header","true").csv(s3_output + "summary")

    return overall_metrics


def plot_metric(df, metric, ax):
    df = df.sort_values("feed_date", axis=0)

    for treatment, color in [("T2", "blue"), ("T3", "green")]:
        group = df[df["treatment"] == treatment][["feed_date", metric]].merge(
            df[df["treatment"] == "T1"][["feed_date", metric]], on="feed_date", how="inner"
        )
        group["diff"] = group[f"{metric}_x"] - group[f"{metric}_y"]
        group.plot(kind="scatter", x="feed_date", y="diff", color=color, s=32, label=treatment, ax=ax)

    ax.axhline(y=0, color='r', linewidth=3)
    ax.set_title(f"{metric} diff with T1", fontsize=16, fontweight="bold")
    ax.tick_params(axis='x', labelrotation=45)


# https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteAccessPermissionsReqd.html
def generate_report(args, region_org, overall_metrics):
    region, org = region_org

    s3 = boto3.resource('s3')
    s3_key_prefix = f"weblab_analysis/solr/{args.feature_name}/{org}/overall/"

    metrics = [
        "sum_rev",
        "impressions",
        "clicks",
        "avg_p_irrel",
        "avg_ectr",
        "actr",
        "avg_cpc",
        "avg_ecvr",
    ]

    fig, axes = plt.subplots(4, 2, sharex=True, sharey=False, figsize=(15, 15))
    fig.tight_layout(pad=5)
    fig.suptitle(f"{args.feature_name} {region_org} weblab analysis", fontsize=24, fontweight="bold")
    fig.subplots_adjust(top=0.9)

    df = overall_metrics.toPandas()
    for metric, ax in zip(metrics, axes.reshape(-1)):
        plot_metric(df, metric, ax)

    img_data = io.BytesIO()
    fig.savefig(img_data, format='png', bbox_inches='tight')
    img_data.seek(0)
    object = s3.Object(s3_bucket, f"{s3_key_prefix}metrics.png")

    # upload the image to s3
    object.put(Body=img_data.read())

'''
Calculate treatment and baseline diff on sum_revenue, impressions, clicks, p_irrel, ectr, actr, cpc, ecvr
'''
def main():
    # Initialization
    args = process_args()

    if args.marketplace_id not in marketplace_mapping:
        raise Exception(f"{args.marketplace_id} is not supported")
    region_org = marketplace_mapping[args.marketplace_id]

    # baseline=T1, treatments = ["T2", "T3"]
    overall_metrics = calculate_overall_metrics(args, region_org)
    generate_report(args, region_org, overall_metrics)


if __name__ == "__main__":
    main()
