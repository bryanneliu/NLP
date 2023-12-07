import warnings, logging, sys

from pyspark.sql import SparkSession

from marketplace_utils import get_marketplace_name

from match_commons import (
    MT_BROAD, MT_EXACT, MT_PHRASE, ATv2_CLOSE, ATv2_LOOSE,
    get_args, set_args_paths,
    filter_output, generate_json_output
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

    # Get candidate data for each type
    # new_close_match = spark.read.parquet(f"{args.tmp_s3_output_path}/horus_raw_after_nkw/{ATv2_CLOSE}/")
    new_loose_match = spark.read.parquet(f"{args.tmp_s3_output_path}/horus_raw_after_nkw/{ATv2_LOOSE}/")
    new_exact_match = spark.read.parquet(f"{args.tmp_s3_output_path}/horus_raw_after_nkw/{MT_EXACT}/")
    new_phrase_match = spark.read.parquet(f"{args.tmp_s3_output_path}/horus_raw_after_nkw/{MT_PHRASE}/")
    new_broad_match = spark.read.parquet(f"{args.tmp_s3_output_path}/horus_raw_after_nkw/{MT_BROAD}/")

    horus_raw_after_nkw = (
        new_loose_match
            #.unionByName(new_close_match)
            .unionByName(new_exact_match)
            .unionByName(new_phrase_match)
            .unionByName(new_broad_match)
    )

    query_ad_count = horus_raw_after_nkw.count()
    query_asin_count = horus_raw_after_nkw.select('searchQuery', 'asin').distinct().count()
    logger.info(f"Before horus_data_filtered, query-ad count: {query_ad_count}, query-ASIN count: {query_asin_count}")

    adtype = args.ad_type
    horus_filtered_dataset = filter_output(spark, logger, args, horus_raw_after_nkw)
    json_output = generate_json_output(horus_filtered_dataset, adtype)

    # Write final Horus data to S3
    logger.info(f"Start to write ad type: {adtype} final Horus format data")
    json_output = json_output.coalesce(64)
    json_output.write.mode("overwrite").json(
        f"{args.output_path}/{adtype}/locale={marketplace_name.upper()}/"
    )
    logger.info(f"Finished writing ad type: {adtype} final Horus format data")


if __name__ == "__main__":
    main()