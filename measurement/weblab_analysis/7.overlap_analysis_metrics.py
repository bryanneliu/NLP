from functools import reduce
from typing import Dict
import logging, sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from pyspark.sql.window import Window

import common as cm

logger = logging.getLogger(__name__)
logging.basicConfig(
    #format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    format="%(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.INFO,
)

QUERY_ID = ['search_query']
ITEM_ID = ['asin', 'marketplace_id']
NORM_QUERY_ID = ['query_norm']

spark = (
    SparkSession.builder.master("yarn")
    .appName("query_overlap_analysis")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext

def aggregate_by_columns(df, col_names):
    df = df.groupBy(*col_names).agg(
            f.count('*').alias('sum_impr'),
            f.round(f.sum("revenue"), 4).alias("sum_revenue"),
            f.round(f.count("asin"), 4).alias("asin_count"),
            f.round(f.sum("click"), 4).alias("click_count"),
            f.round(f.avg("click"), 4).alias("avg_ctr"),
            f.round(f.avg("p_irrel"), 4).alias("avg_p_irrel"),
            f.round(f.avg("ECTR"), 4).alias("avg_ectr"),
            f.round(f.avg("ECVR"), 4).alias("avg_ecvr"),
            f.round(f.avg("cpc"), 4).alias("avg_cpc"),
            f.round(f.sum("ops"), 4).alias("sum_ops"),
            f.round(f.sum("ops")*1.0/f.sum("revenue"), 4).alias("avg_roas"),
            f.round(f.count('*')*1.0/f.countDistinct("http_request_id"), 4).alias("ad_density"),
            f.countDistinct('asin', 'advertiser_id').alias('asin_advid_count')
        )
    return df

def add_shares_columns(df):
    df = (
        df.withColumn('rev_share', f.col('sum_revenue')/f.sum('sum_revenue').over(Window.partitionBy("treatment")))
        .withColumn('impr_share', f.col('sum_impr')/f.sum('sum_impr').over(Window.partitionBy("treatment")))
        .withColumn('avg_rev_per_impr', f.col('sum_revenue')/f.col('sum_impr'))
    )
    return df

def get_treatment_impressed_ads(args, treatment, impressed_ad_details) -> DataFrame:
    impressed_ad_details = impressed_ad_details.filter(f"treatment = '{treatment}'")
    query_requests = impressed_ad_details.groupBy(*QUERY_ID).agg(
        f.countDistinct("http_request_id").alias("total_requests"))
    # Drop results not attributed to adtype
    adtype_impressed_ads = impressed_ad_details.filter(f"ad_type != 'Other'")
    adtype_impressed_ads = adtype_impressed_ads.join(query_requests, on=QUERY_ID, how='left')
    return adtype_impressed_ads


def get_query_aggregates(wl_log, aggr_cols=QUERY_ID):
    # Normalises metrics per request in treatment for fair comparison
    per_request_norm = lambda column_name: (f.col(column_name) / f.col('total_requests')).alias(f'{column_name}_norm')
    query_aggregates = wl_log.groupBy(aggr_cols).agg(
        f.countDistinct('asin').alias('asins'),
        f.countDistinct('http_request_id').alias('adtype_requests'),
        f.sum('click').alias('clicks'),
        f.count('*').alias('impressions'),
        f.sum('revenue').alias('revenue'),
        f.avg('ectr').alias('ectr'),
        (f.sum('click') / f.count('*')).alias('actr'),
        f.first('total_requests').alias('total_requests'),
        f.collect_set(f.when(f.col('click') > 0, f.col('asin')).otherwise(f.lit(None))).alias("clicked_asins")
    )
    request_norm_cols = ['revenue', 'clicks', 'impressions']
    request_norm_exprs = [per_request_norm(column) for column in request_norm_cols]

    # Adding normalized metrics for comparison. Meanwhile, impressions and revenue can be used to sort by impact etc.
    return query_aggregates.select(*query_aggregates.columns, *request_norm_exprs)


def get_overlap_dataset(treatment_aggrs: Dict[str, DataFrame], join_cols=QUERY_ID):
    arm_prefix = lambda columns, arm: join_cols + [f.col(column).alias(f'{column}_{arm}') for column in
                                                   set(columns).difference(set(join_cols))]
    treatment_aggrs = {
        treatment: aggrs.select(arm_prefix(aggrs.columns, treatment)).withColumn(f'in_{treatment}', f.lit(1))
        for treatment, aggrs in treatment_aggrs.items()}
    # Include non-overlapping queries to be filtered out as needed at analysis time
    aggr_join = lambda x, y: x.join(y, on=join_cols, how='outer')
    return reduce(aggr_join, treatment_aggrs.values())


def get_args():
    parser = cm.get_default_argparser()
    parser.add_argument(
        "--treatments",
        type=str,
        default="T1,T2",
        help="Treatments to be chosen for overlap analysis separated by ','."
             "Common case scenario is to use two treatments, but more than two is supported too. "
             "For generating metrics, only first 2 treatments are used, assuming first is the baseline",
        required=True
    )

    return parser.parse_known_args()

def generate_metrics(args, impressed_ad_details, treatments):
    ''' For impressions sourced by this adtype, we compute metrics on overlapped queries vs. incremental
    '''
    baseline_treatment, cand_treatment = treatments
    adtype_queries = impressed_ad_details.filter(f"treatment = '{cand_treatment}'").filter(
        (f.col('ad_type') == (args.ad_type.lower()+"Only")) | (f.col('ad_type') == (args.ad_type.lower()+"Overlap"))
        ).select("search_query").distinct()
    baseline_queries = (
        impressed_ad_details
            .filter(f"treatment = '{baseline_treatment}'")
            .select(f.col("search_query").alias('baseline_query'))
            .distinct()
    )
    adtype_queries = (
        adtype_queries
            .join(baseline_queries, f.col("search_query")==f.col('baseline_query'), 'left')
            .withColumn('query_overlap_flag', 
                        f.when(f.col('baseline_query').isNotNull(), 'overlap_with_baseline').otherwise('adtype_incremental'))
            .drop('baseline_query')
    )

    adtype_query_impressions = impressed_ad_details.join(adtype_queries, QUERY_ID, 'inner')
    distribution = add_shares_columns(
        aggregate_by_columns(adtype_query_impressions, ["treatment", "query_overlap_flag", "ad_type"])
    )
    logger.info("#######################")
    logger.info("Adtype sourced queries, overlap with C vs. incremental comparison")
    logger.info("#######################")
    logger.info("Sort by treatment and query_overlap_flag desc")
    distribution.sort(f.col('treatment'), f.col('query_overlap_flag'), f.col('ad_type')).show(200, False)


def get_treatments(args):
    treatments = [x.strip() for x in args.treatments.split(",")]
    if len(treatments) <= 1:
        raise ValueError(f"Require more than one ',' separated treatment. Input: {args.treatments}")

    return treatments


def main():
    args, _ = get_args()
    logger.info(f"Starting up analysis with args: {args}")
    treatments = get_treatments(args)
    impressed_ad_details = cm.load_impressed_ad_details(spark, args)
    
    # Generate queries set with overlap flag
    treatment_aggrs = {treatment: get_query_aggregates(get_treatment_impressed_ads(args, treatment, impressed_ad_details), QUERY_ID)
                       for treatment in treatments}
    all_queries = get_overlap_dataset(treatment_aggrs, join_cols=QUERY_ID)
    logger.info("============ Sample queries set: ============")
    all_queries.show(10, False)
    output_location = f"s3://{cm.s3_bucket}/overlap_analysis/{args.Solr_or_Horus.lower()}/{args.ad_type}/{args.weblab_name}/{args.marketplace_id}/" \
                      f"{args.start_date}_{args.end_date}/{'_'.join(treatments)}/all_queries/"
    all_queries.write.mode('overwrite').parquet(output_location)
    
    # generate metrics, assuming first treatment is the baseline
    if len(treatments) > 2:
        logger.warning(f"Input treatments {treatments} more than 2, only first 2 are used in metric generation.")
    generate_metrics(args, impressed_ad_details, treatments[:2])

if __name__ == "__main__":
    main()
