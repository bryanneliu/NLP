import warnings, logging, sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, broadcast

from marketplace_utils import get_marketplace_name

from match_commons import (
    get_args, set_args_paths, register_horus_tokenization_match_udf, ATv2_LOOSE,
    get_janus_qdf_data, prepare_output
)

warnings.simplefilter(action="ignore", category=FutureWarning)
logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.INFO)

spark = (
    SparkSession.builder.master("yarn")
    .appName("Add_Tommy_Addition_Loose_Match")
    .enableHiveSupport()
    .getOrCreate()
)


def main():
    # Prepare args
    args = get_args()
    logger.info(f"Start, arguments are {args}")

    marketplace_name = get_marketplace_name(args.marketplace_id)
    logger.info(f"Janus marketplace name: {marketplace_name}")

    set_args_paths(args, logger)

    register_horus_tokenization_match_udf(spark)

    # Get Tommy addition query-ASIN
    tommy_addition = spark.read.parquet(args.s3_tommy_addition_latest)
    janus_asins = spark.read.parquet(args.s3_janus_asins_latest)
    head_asins = janus_asins.where(col("reviewCount") >= args.review_count_threshold).select("asin")
    query_asin = tommy_addition.join(broadcast(head_asins), ["asin"], "inner")

    # Get AT loose match <asin, ad>
    janus_mt, janus_at, janus_nkw = get_janus_qdf_data(spark, args, logger, marketplace_name)
    loose_match = janus_at.where(col("expression") == ATv2_LOOSE)

    new_loose_match = query_asin.join(loose_match, ["asin"], "inner")

    # Prepare the output & run NKW rules
    prepare_output(spark, logger, args, new_loose_match, janus_nkw, ATv2_LOOSE)
    janus_nkw.unpersist()


if __name__ == "__main__":
    main()