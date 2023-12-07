from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import logging

import common as cm

logger = logging.getLogger()
logger.setLevel(logging.INFO)

spark = (
    SparkSession.builder.master("yarn")
    .appName("Dataset ASINs performance")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext


def read_asin_dataset(path, token_column, marketplace_id):
    token_dataset = spark.read.parquet(path).filter(f"marketplaceId = '{marketplace_id}'")\
        .withColumnRenamed('marketplaceId', 'marketplace_id')
    token_dataset = token_dataset.withColumn(token_column, f.split(token_column, '\s+')) \
        .withColumn("token_count", f.size(token_column))
    token_dataset = token_dataset.withColumn("token_rank",
                                             f.percent_rank().over(Window.orderBy(f.col('token_count').desc()))) \
        .drop(token_column)
    return token_dataset



def join_impressions_tokens(impression_log, token_dataset):
    token_dataset = token_dataset.withColumn('in_dataset', f.lit(True))
    log_tokens = impression_log.join(token_dataset, on=['asin', 'marketplace_id'], how='left')
    log_tokens = log_tokens.withColumn("token_decile",
                                       f.coalesce(f.ceil(f.col('token_rank') * 10) * 10, f.lit('notoken')))
    log_tokens = log_tokens.withColumn('in_dataset', f.coalesce(f.col('in_dataset'), f.lit(False)))

    return log_tokens

def get_args():
    parser = cm.get_default_argparser()
    parser.add_argument(
        "--treatments",
        type=str,
        default="T1,T2",
        help="Treatments to compare in the delta performance",
        required=True
    )

    parser.add_argument(
        "--token_dataset",
        type=str,
        help="S3 path to token dataset of ASINs",
        required=True
    )

    parser.add_argument(
        "--token_column",
        type=str,
        help="Name of the column in dataset with tokens",
        required=True
    )

    return parser.parse_known_args()


"""
Script to generate metrics based on Solr ASIN token datasets. Skipped for Horus weblabs. Currently generates:
1. Token count decile based performance
2. Performance metrics for ASINs in (vs) not in dataset

Potential additions: Generate top and bottom query overlap tokens for future iterations
"""
def main():
    args, _ = get_args()

    if not cm.is_solr_weblab(args):
        logger.info(f"Skipping dataset ASIN analysis for non-Solr weblab: {args}")
        return

    if not args.token_dataset:
        logger.info(f"Skipping Solr token dataset performance evaluation since the path was not provided: {args}")
        return

    impression_log = cm.load_impressed_ad_details(spark, args).withColumn("marketplace_id", f.lit(args.marketplace_id))

    impression_log = impression_log.filter(f.col('treatment')
                                           .isin(*[f.lit(treatment) for treatment in args.treatments.split(',')]))

    token_dataset = read_asin_dataset(args.token_dataset, args.token_column, args.marketplace_id)
    impression_tokens = join_impressions_tokens(impression_log, token_dataset)

    reports_root = cm.get_output_root(args)

    token_decile_aggrs = cm.get_cohort_aggregates(impression_tokens, cohorts=['marketplace_id', 'treatment', 'token_decile'])
    token_decile_delta = cm.calculate_metric_delta(token_decile_aggrs, cohorts=['token_decile'])
    token_decile_report_path = f'{reports_root}/tokens/token_decile'
    cm.save_report(token_decile_delta, token_decile_report_path)
    logger.info(f"Finished writing token decile performance report: {token_decile_report_path}")

    covered_asin_aggrs = cm.get_cohort_aggregates(impression_tokens, cohorts=['marketplace_id', 'treatment', 'in_dataset'])
    covered_asin_delta = cm.calculate_metric_delta(covered_asin_aggrs, cohorts=['in_dataset'])
    covered_asin_delta_path = f'{reports_root}/tokens/covered_asin'
    cm.save_report(covered_asin_delta, covered_asin_delta_path)
    logger.info(f"Finished writing dataset ASIN performance report: {covered_asin_delta_path}")


if __name__ == "__main__":
    main()