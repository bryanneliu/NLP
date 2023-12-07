import warnings
import logging, sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from marketplace_utils import get_marketplace_name

from tokenizer_enhanced import (
    set_spear_tokenizer_resources
)

from match_commons import (
    MT_PHRASE, SOURCE_CAP, SOURCE_TOMMY_ADDITION,
    get_args, set_args_paths, register_horus_tokenization_match_udf, is_phrase_match_UDF,
    get_match_candidates, dump_exact_phrase_samples, prepare_output
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
    .appName("Improve_CAP_phrase_match")
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

    # Register old and new tokenizer
    register_horus_tokenization_match_udf(spark)
    set_spear_tokenizer_resources(spark)

    # Get phrase match candidates
    phrase_match_candidates, janus_nkw = get_match_candidates(spark, args, logger, marketplace_name, MT_PHRASE)
    janus_nkw.cache()

    phrase_match_candidates = (
        phrase_match_candidates
            .withColumn(
                "is_txt_norm_phrase_match",
                is_phrase_match_UDF(col("txt_norm_expression"), col("txt_norm_query"))
            )
    )

    # Only apply improvements to non-prod CAP phrase match
    candidates_to_improve = (
        phrase_match_candidates
            .where(
                ((col("source") == SOURCE_CAP) & (col("is_txt_norm_phrase_match") == 0)) |
                (col("source") == SOURCE_TOMMY_ADDITION)
            )
            .withColumn(
                "is_normalized_phrase_match",
                is_phrase_match_UDF(col("normalized_expression"), col("normalized_query"))
            )
    )

    # Get new phrase matches
    new_phrase_match = (
        candidates_to_improve
            .where(
                (col("is_txt_norm_phrase_match") == 1) |
                (col("is_normalized_phrase_match") == 1)
            )
    ).cache()

    if args.verbose_mode:
        dump_exact_phrase_samples(args, new_phrase_match, MT_PHRASE)

        cap_new_phrase_match = new_phrase_match.where(col("source") == SOURCE_CAP)
        tommy_addition_new_phrase_match = new_phrase_match.where(col("source") == SOURCE_TOMMY_ADDITION)
        logger.info(f"cap_new_phrase_match count: {cap_new_phrase_match.count()}")
        logger.info(f"tommy_addition_new_phrase_match count: {tommy_addition_new_phrase_match.count()}")


    # Prepare the output & run NKW rules
    # With less data to join does not help with the performance
    # Separate new_phrase_match to cap_new_phrase_match & tommy_addition_new_phrase_match does not help
    prepare_output(spark, logger, args, new_phrase_match, janus_nkw, MT_PHRASE)
    new_phrase_match.unpersist()
    janus_nkw.unpersist()


if __name__ == "__main__":
    main()


