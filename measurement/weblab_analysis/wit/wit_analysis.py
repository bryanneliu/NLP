import argparse
import logging
import sys

from pyspark.sql import SparkSession

from pyspark.sql import functions as f
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType
from collections import defaultdict

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
    .appName("wit_analysis")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext

def process_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--cohort_file_path",
        type=str,
        default="s3://spear-team/lbryanne/SPONSORED_PRODUCTS_536837/wit/*/",
        help="/wit/marketplace/cohort.csv",
        required=True
    )

    parser.add_argument(
        "--control",
        type=str,
        default="T1",
        help="Control of Solr experiments is T1, Control of Horus experiments is C",
        required=True
    )

    parser.add_argument(
        "--default_treatment",
        type=str,
        default="T2",
        help="Treatment for majority of markets",
        required=True
    )

    parser.add_argument(
        "--market_treatment_override_str",
        type=str,
        default="JP:T3;SG:T3;AU:T3",
        help="Specify with format marketplace1:treatment;marketplace2:treatment;",
        required=False
    )

    parser.add_argument(
        "--control_allocation",
        type=int,
        default=25,
        help="25% of traffic with Control arm",
        required=True
    )

    parser.add_argument(
        "--treatment_allocation",
        type=int,
        default=25,
        help="25% of traffic with Treatment arm",
        required=True
    )

    parser.add_argument(
        "--top_impression_share",
        type=float,
        default=0.1,
        help="We report all segments that have impression share >=10% for the marketplace to simplify the tables and reduce noise",
        required=True
    )

    parser.add_argument(
        "--s3_output_folder",
        type=str,
        default="s3://spear-team/lbryanne/SPONSORED_PRODUCTS_536837/wit/aggregated/",
        help="S3 output folder for aggregated WIT analysis",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


cohorts = [
    ("ach_prime_status_onlineBusiness.csv", "ach_prime_status"),
    ("is_recognized_customer_onlineBusiness.csv", "is_recognized_customer"),
    ("shopper_lifecycle_segment_onlineBusiness.csv", "shopper_lifecycle_segment"),
    ("shopper_purchase_frequency_onlineBusiness.csv", "shopper_purchase_frequency"),
    ("shopper_purchase_recency_onlineBusiness.csv", "shopper_purchase_recency"),
    ("shopper_tenure_onlineBusiness.csv", "shopper_tenure"),
    ("shopper_age_onlineBusiness.csv", "shopper_age"),
    ("shopper_gender_onlineBusiness.csv", "shopper_gender"),

    ("marketplace_id_onlineBusiness.csv", "marketplace_id"),
    ("page_type_onlineBusiness.csv", "page_type"),
    ("query_revenue_decile_onlineBusiness.csv", "query_revenue_decile"),
    ("advertiser_revenue_decile_onlineBusiness.csv", "advertiser_revenue_decile"),
    ("advertiser_roas_decile_onlineBusiness.csv", "advertiser_roas_decile"),
    ("widget_name_onlineBusiness.csv", "widget_name"),
    ("gl_product_onlineBusiness.csv", "gl_product"),
    ("shopper_query_broadness_onlineBusiness.csv", "shopper_query_broadness"),
    ("ops_ranked_shoppers_cohort_onlineBusiness.csv", "ops_ranked_shoppers_cohort")
]


def select_row_udf(market_treatment):
    def select_row(market, treatment, market_treatment):
        return market in market_treatment and treatment == market_treatment[market]

    return udf(lambda market, treatment: select_row(market, treatment, market_treatment), BooleanType())


def get_market_treatment(default_treatment, markets, market_treatment_override):
    market_treatment = defaultdict(str)
    for market in markets:
        if market in market_treatment_override:
            market_treatment[market] = market_treatment_override[market]
        else:
            market_treatment[market] = default_treatment

    return market_treatment


def get_market_treatment_override(market_treatment_override_str):
    market_treatment_override = defaultdict(str)
    if not market_treatment_override_str or market_treatment_override_str.lower() == "none":
        return market_treatment_override

    for market_treatment in market_treatment_override_str.split(";"):
        if market_treatment:
            market, treatment = market_treatment.split(":")
            market_treatment_override[market] = treatment

    return market_treatment_override


# Compare with control
# We report all segments that have impression share >=10% for the marketplace to simplify the tables and reduce noise
# scaling_factor = 1
# diff = Treatment/Control - 1
def wit_analysis_with_same_allocation(cohort_file_path, s3_output_folder, markets, default_treatment, market_treatment_override,
                                      top_impression_share=0.1):
    # Get targeted treatments for all markets
    market_treatment = get_market_treatment(default_treatment, markets, market_treatment_override)
    targeted_treatment_udf = select_row_udf(market_treatment)

    for cohort in cohorts:
        logger.info(cohort[0].replace(".csv", ""))

        s3_cohort_csv = cohort_file_path + f"{cohort[0]}"
        try:
            wit = spark.read.csv(s3_cohort_csv, header=True)
        except Exception as e:
            continue

        total_impressions = wit.groupBy("marketplace", "treatment").agg(
            f.sum("impressions").alias("total_impressions")
        )

        wit = (
            wit
            .join(total_impressions, ["marketplace", "treatment"], "inner")
            .withColumn("impression_share", col("impressions") / col("total_impressions"))
            .where(col("impression_share") >= top_impression_share)
            .withColumn("is_targeted", targeted_treatment_udf(col("marketplace"), col("treatment")))
            .where(col("is_targeted") == True)
        )

        segment = cohort[1]
        row_ids = ["marketplace", "treatment", segment]

        pretty_num = lambda num, d=4: f.round(num, d)
        metrics = ["impression_share", "roas_scaled_diff", "revenue_scaled_diff", "queries_scaled_diff",
                   "ops_scaled_diff", "impressions_scaled_diff", "cpc_scaled_diff", "avg_pirrel_scaled_diff",
                   "asins_scaled_diff", "advertisers_scaled_diff", "ad_density_scaled_diff", "acvr_scaled_diff",
                   "actr_scaled_diff"]
        normalized_metrics = [pretty_num(f.col(metric)).alias(metric.replace("_scaled", "")) for metric in metrics]

        wit = wit.select(*row_ids, *normalized_metrics).sort(col("marketplace"), col("impression_share").desc())
        wit.show(100, False)
        wit = wit.coalesce(1)
        (
            wit.write
                .mode("overwrite")
                .option("delimiter", "\t")
                .option("header", True)
                .csv(f"{s3_output_folder}{cohort[0]}")
        )


metrics_to_update = ["impressions", "revenue", "ops", "clicks"]

metrics_not_to_update = [
    "cpc_scaled_diff", "roas_scaled_diff", "avg_pirrel_scaled_diff", "ad_density_scaled_diff", "acvr_scaled_diff", "actr_scaled_diff"
]


def recalculate_diff(wit, control, segment, allocation_ratio):
    control_analysis = (
        wit.where(col("treatment") == control)
        .select(["marketplace", segment] + [metric for metric in metrics_to_update])
    )

    for metric in metrics_to_update:
        control_analysis = control_analysis.withColumn(metric + "_C", col(metric).cast("float"))
    control_analysis = control_analysis.drop(*metrics_to_update)

    treatment_analysis = wit.where(col("treatment") != control)
    analysis = treatment_analysis.join(control_analysis, ["marketplace", segment], "inner")

    analysis = (
        analysis.select(
            *["marketplace", "treatment", segment, "impressions"],
            *metrics_not_to_update,
            *[(f.col(metric) * allocation_ratio / f.col(metric + "_C") - 1).alias(metric + "_scaled_diff") for
              metric in metrics_to_update]
        )
    )

    return analysis


# Compare with control
# We report all segments that have impression share >=10% for the marketplace to simplify the tables and reduce noise
# scaling_factor = 1
# diff = Treatment/Control - 1
def wit_analysis_with_different_allocation(cohort_file_path, s3_output_folder, markets, control, default_treatment,
                                           market_treatment_override, allocation_ratio, top_impression_share=0.1):
    market_treatment = get_market_treatment(default_treatment, markets, market_treatment_override)
    targeted_treatment_udf = select_row_udf(market_treatment)

    for cohort in cohorts:
        logger.info(cohort[0].replace(".csv", ""))

        s3_cohort_csv = cohort_file_path + f"{cohort[0]}"
        try:
            wit = spark.read.csv(s3_cohort_csv, header=True)
        except Exception as e:
            continue

        # re-calculate diff for treatments
        segment = cohort[1]
        wit = recalculate_diff(wit, control, segment, allocation_ratio)

        total_impressions = wit.groupBy("marketplace", "treatment").agg(
            f.sum("impressions").alias("total_impressions")
        )

        wit = (
            wit
                .join(total_impressions, ["marketplace", "treatment"], "inner")
                .withColumn("impression_share", col("impressions") / col("total_impressions"))
                .where(col("impression_share") >= top_impression_share)
                .withColumn("is_targeted", targeted_treatment_udf(col("marketplace"), col("treatment")))
                .where(col("is_targeted") == True)
        )

        row_ids = ["marketplace", "treatment", segment]

        pretty_num = lambda num, d=4: f.round(num, d)
        metrics = ["impression_share"] + [metric + "_scaled_diff" for metric in
                                          metrics_to_update] + metrics_not_to_update
        normalized_metrics = [pretty_num(f.col(metric)).alias(metric.replace("_scaled_diff", "_lift")) for metric in
                              metrics]

        wit = wit.select(*row_ids, *normalized_metrics).sort(col("marketplace"), col("impression_share").desc())
        wit.show(100, False)

        wit = wit.coalesce(1)
        (
            wit.write
                .mode("overwrite")
                .option("delimiter", "\t")
                .option("header", True)
                .csv(f"{s3_output_folder}{cohort[0]}")
        )


def main():
    # Initialization
    args = process_args()

    # Input and Output folder
    cohort_file_path = args.cohort_file_path
    s3_output_folder = args.s3_output_folder

    # Control and Treatment for each market
    control = args.control
    default_treatment = args.default_treatment
    market_treatment_override_str = args.market_treatment_override_str
    market_treatment_override = get_market_treatment_override(market_treatment_override_str)

    # Control and Treatment allocation, allocation_ratio
    control_allocation = args.control_allocation
    treatment_allocation = args.treatment_allocation
    allocation_ratio = control_allocation / treatment_allocation

    # Show top impression share
    top_impression_share = args.top_impression_share

    # Top 9 markets
    top_markets = ["US", "CA", "UK", "DE", "FR", "ES", "IN", "IT", "JP"]
    # Non-top 12 markets
    non_top_markets = ["AE", "AU", "BE", "BR", "MX", "EG", "NL", "TR", "PL", "SA", "SE", "SG"]

    if allocation_ratio == 1:
        logger.info("####################################")
        logger.info("WIT analysis for top 9 markets:")
        logger.info("####################################")
        wit_analysis_with_same_allocation(cohort_file_path, s3_output_folder+"top_markets/", top_markets, default_treatment, market_treatment_override, top_impression_share)
        logger.info("####################################")
        logger.info("WIT analysis for non-top 12 markets:")
        logger.info("####################################")
        wit_analysis_with_same_allocation(cohort_file_path, s3_output_folder+"non_top_markets/", non_top_markets, default_treatment, market_treatment_override, top_impression_share)
    else:
        logger.info("####################################")
        logger.info("WIT analysis for top 9 markets:")
        logger.info("####################################")
        wit_analysis_with_different_allocation(cohort_file_path, s3_output_folder+"top_markets/", top_markets, control, default_treatment,
                                           market_treatment_override, allocation_ratio, top_impression_share)
        logger.info("####################################")
        logger.info("WIT analysis for non-top 12 markets:")
        logger.info("####################################")
        wit_analysis_with_different_allocation(cohort_file_path, s3_output_folder+"non_top_markets/", non_top_markets, control, default_treatment,
                                           market_treatment_override, allocation_ratio, top_impression_share)


if __name__ == "__main__":
    main()
