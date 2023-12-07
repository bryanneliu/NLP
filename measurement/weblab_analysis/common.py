from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import ArrayType, StringType, IntegerType
from pyspark.sql.functions import udf
from datetime import datetime, timedelta
import argparse
from functools import reduce

from marketplace_utils import get_marketplace_name

Janus_snapshot_S3_prefix = {
    "EU": "s3://cpp-janus-eu-prod-sp-redrive-json/EUAmazon/",
    "FE": "s3://cpp-janus-fe-prod-sp-redrive-json/FEAmazon/",
    "NA": "s3://cpp-janus-na-prod-sp-redrive-json/USAmazon/"
}

s3_bucket = "spear-measurement"
s3_weblab_analysis = "s3://spear-measurement/weblab_analysis/"

"""
Arg parser with default argument expected. Scripts can add more args as needed
"""
def get_default_argparser():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--marketplace_id",
        type=int,
        default=1,
        help="Specify solr_index marketplaceid",
        required=True
    )

    parser.add_argument(
        "--start_date",
        type=str,
        default="2022-12-04",
        help="Specify date to analyze on",
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
        "--feed_glue_table",
        type=str,
        default="spear_lbryanne.spear_weblab_logging",
        help="Use FEED to pull weblab data into this glue table, query it for weblab impression data",
        required=True
    )

    parser.add_argument(
        "--weblab_name",
        type=str,
        default="SPONSORED_PRODUCTS_546406",
        help="Use as filtering, and as part of output path",
        required=True
    )

    parser.add_argument(
        "--Solr_or_Horus",
        type=str,
        default="solr",
        choices=["solr", "horus", "Solr", "Horus"],
        help="Trigger different steps for Solr and Horus",
        required=True
    )

    parser.add_argument(
        "--ad_type",
        type=str,
        default="Search",
        help="Each data source has an ad_type, Search for Solr, RELAX2 for query relaxation",
        required=True
    )
    return parser


def process_args():
    parser = get_default_argparser()
    args, _ = parser.parse_known_args()
    return args

def s3_path_exists(sc, path):
    # spark is a SparkSession
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
        sc._jsc.hadoopConfiguration(),
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))


def get_token_count(expression):
    if not expression:
        return 0
    return len(expression.split(" "))
get_token_count_UDF = udf(get_token_count, IntegerType())


# Solr: ad_type = Search
def get_ad_type(ad_types_string, ad_type):
    if not ad_types_string or not ad_type:
        return "None"

    ad_types_string = ad_types_string.lower()
    ad_type = ad_type.lower()
    if ad_types_string == ad_type:
        return ad_type + "Only"

    ad_types = set(ad_types_string.split(","))
    if ad_type in ad_types:
        return ad_type + "Overlap"

    return "Other"
get_ad_type_UDF = udf(get_ad_type, StringType())


# DO NOT USE "%Y/%m/%d" for output folder unless it's your intention
def format_date(date_str, date_format):
    if date_str:
        if "-" in date_str:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        elif "/" in date_str:
            dt = datetime.strptime(date_str, "%Y/%m/%d")
        else:
            dt = datetime.strptime(date_str, "%Y%m%d")
    else:
        dt = datetime.now()

    if date_format:
        return dt.strftime(date_format)

    return dt.strftime("%Y-%m-%d")


def report_percent(num, denom, d=2):
    return F.round(num / denom * 100, d)


def report_delta(before, after, d=2):
    return report_percent(after - before, before, d)

def save_report(report_df, path):
    return report_df.repartition(1).write.mode("overwrite").option("header", "true").csv(path)

def get_output_root(args):
    marketplace = get_marketplace_name(args.marketplace_id).upper()
    return f"{s3_weblab_analysis}{args.Solr_or_Horus.lower()}/{args.weblab_name}/{marketplace}"

def get_treatment_delta(baseline, treatment, cohorts):
    row_id = set(cohorts).union({"marketplace_id"})
    metric_columns = set(treatment.columns).difference(row_id.union({"treatment"}))
    treatment_cols = [F.col(metric).alias(f"{metric}_tr") for metric in metric_columns]
    treatment = treatment.select(*row_id, *treatment_cols)

    joined = baseline.join(treatment, on=list(row_id), how="outer")

    TOPLINE_METRICS = {'impressions', 'clicks', 'revenue'}
    # Include difference in topline metrics to understand weights of cohorts
    diff_cols = [(F.col(f'{metric}_tr') - F.col(metric)).alias(f'{metric}_diff')
                 for metric in metric_columns.intersection(TOPLINE_METRICS)]
    delta_cols = [report_delta(F.col(metric), F.col(f"{metric}_tr"), 3).alias(metric) for metric in metric_columns]
    delta_report = joined.select(*row_id, *delta_cols, *diff_cols)
    return delta_report

def is_solr_weblab(args):
    return args.Solr_or_Horus.lower() == 'solr'

def get_cohort_aggregates(impression_logs, cohorts):
    aggrs = impression_logs.groupby(*cohorts).agg(
        F.mean(F.col('click')).alias('actr'),
        F.mean(F.col('cpc')).alias('avg_cpc'),
        F.mean(F.col('p_irrel')).alias('avg_p_irrel'),
        F.mean(F.col('ECTR')).alias('avg_ectr'),
        F.mean(F.col('ECVR')).alias('avg_ecvr'),
        (F.round(F.sum('ops') / F.sum('revenue'), 6)).alias('roas'),
        F.countDistinct('asin').alias('asins'),
        F.countDistinct('asin', 'advertiser_id').alias('asin_advid_count'),
        F.countDistinct('http_request_id').alias('requests'),
        F.countDistinct('advertiser_id').alias('advertisers'),
        F.sum('revenue').alias('revenue'),
        F.sum('ops').alias('ops'),
        F.count('*').alias('impressions'),
        F.sum('click').alias('clicks'))
    aggrs = aggrs.withColumn('cpc', F.col('revenue') / F.col('clicks'))
    return aggrs

def calculate_metric_delta(cohort_aggrs, cohorts, baseline="T1"):
    treatments = [tr_row[0] for tr_row in cohort_aggrs.select("treatment").distinct().collect()]
    arm_data = {treatment: cohort_aggrs.filter(f"treatment = '{treatment}'") for treatment in treatments}
    baseline_df = arm_data.pop(baseline)

    treatment_deltas = {treatment: get_treatment_delta(baseline_df, arm_data[treatment], cohorts)
    .withColumn("treatment", F.lit(treatment)) for treatment in arm_data}
    return reduce(DataFrame.union, list(treatment_deltas.values()))


def load_impressed_ad_details(spark: SparkSession, args):
    marketplace = get_marketplace_name(args.marketplace_id).upper()
    S3_searchWeblabJoinJanus_folder = f"{s3_weblab_analysis}{args.Solr_or_Horus.lower()}/{args.weblab_name}/{marketplace}/daily/"

    current_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    last_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    days = (last_date - current_date).days
    date_str_list = [str(current_date + timedelta(days=d)) for d in range(days+1)]
    paths = [S3_searchWeblabJoinJanus_folder + f"{date_str}/searchWeblabJoinJanus/" for date_str in date_str_list]
    data = spark.read.parquet(*paths)

    return data
