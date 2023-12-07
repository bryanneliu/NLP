import warnings, logging, sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array

from marketplace_utils import get_marketplace_name

from tokenizer_enhanced import (
    set_spear_tokenizer_resources
)

from synonym import (
    load_reviewed_synonym, synonym_expansion_with_synonym_dict
)

from match_commons import (
    SOURCE_CAP, SOURCE_TOMMY_ADDITION, ATv2_CLOSE,
    get_args, set_args_paths, register_horus_tokenization_match_udf,
    is_full_match_udf, is_range_full_match_udf,
    get_close_match_candidates, prepare_output
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
    .appName("Improve_CAP_Close_Match")
    .enableHiveSupport()
    .getOrCreate()
)


# Just keep for the future use
def load_query_relaxation_data(args):
    '''
    +-----------------------------------------------------------------+--------------------------+-------------+------------------+----------------------------+-------------------+--------------------------+
    |original_query                                                   |query_relaxation          |request_count|score             |query_relaxation_token_count|reduced_token_count|original_query_token_count|
    +-----------------------------------------------------------------+--------------------------+-------------+------------------+----------------------------+-------------------+--------------------------+
    |Clinique Clarifying Lotion - Skin Type 3, 6.7 oz                 |clinique clarifying lotion|3            |0.8778310710199569|3                           |6                  |9                         |
    |squishmallows fruit veggie squad 8" scarlet strawberry plush doll|squishmallows fruit squad |3            |0.7504793983582456|3                           |6                  |9                         |
    +-----------------------------------------------------------------+--------------------------+-------------+------------------+----------------------------+-------------------+--------------------------+
    '''
    s3_qr = "s3://multi-query/query_relaxation/US/combined/qr/"
    qr = spark.read.parquet(s3_qr)

    qr = qr.where(
        (col("original_query_token_count") >= 3) &
        (col("original_query_token_count") <= 10) &
        (col("query_relaxation_token_count") >= 2) &
        (col("original_query_token_count") > col("query_relaxation_token_count")) &
        (col("reduced_token_count") < 3)
    )

    qr.createOrReplaceTempView("qr")
    qr_analyzed = (
        spark.sql(
            f"""SELECT DISTINCT txt_norm(original_query, '{args.marketplace_id}') AS tokenized_query, 
                                txt_norm(query_relaxation, '{args.marketplace_id}') AS tokenized_query_relaxation
                FROM qr"""
        )
    )

    return qr_analyzed


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

    # Support broad tokenizer & synonym on query, title full match
    close_match_candidates, janus_nkw = get_close_match_candidates(spark, args, logger, marketplace_name)
    janus_nkw.cache()

    close_match_candidates = (
        close_match_candidates
            .withColumn(
                "is_txt_norm_full_match",
                is_full_match_udf(col("txt_norm_query"), col("txt_norm_title"))
            )
    )

    # Synonym Expansion on query, expression and query full match
    synonym_dict = load_reviewed_synonym(spark, logger)
    synonym_expansion_udf = synonym_expansion_with_synonym_dict(synonym_dict)

    # Only apply the improvements to non-prod cap broad match
    candidates_to_improve = (
        close_match_candidates
            .where(
                ((col("source") == SOURCE_CAP) & (col("is_txt_norm_full_match") == 0)) |
                (col("source") == SOURCE_TOMMY_ADDITION)
            )
            .withColumn(
                "is_full_match",
                is_full_match_udf(col("broad_normalized_query"), col("broad_normalized_title"))
            )
            .withColumn(
                "synonym_expansion",
                synonym_expansion_udf(col("txt_norm_query"), col("txt_norm_title"))
            )
    )

    '''
        .withColumn(
            "is_range_full_match",
            is_range_full_match_udf(col("broad_normalized_query"), col("broad_normalized_query"), col("has_num_pattern"))
        )
    '''

    # Get new close matches
    new_close_match = (
        candidates_to_improve
            .where(
                (col("is_txt_norm_full_match") == 1) |
                (col("is_full_match") == 1) |
                (col("synonym_expansion.is_full_match_with_synonym") == 1)
            )
    )

    if args.verbose_mode:
        cap_old_broad_match = (
            close_match_candidates
            .where(
                (col("source") == SOURCE_CAP) &
                (col("is_txt_norm_full_match") == 1)
            )
        )
        logger.info(f"Old cap broad match (before nkw filtering) count: {cap_old_broad_match.count()}")

        cap_new_close_match = new_close_match.where(col("source") == SOURCE_CAP)
        tommy_addition_new_close_match = new_close_match.where(col("source") == SOURCE_TOMMY_ADDITION)
        logger.info(f"new_close_match count: {new_close_match.count()}")
        logger.info(f"cap_new_close_match count: {cap_new_close_match.count()}")
        logger.info(f"tommy_addition_new_close_match count: {tommy_addition_new_close_match.count()}")

        logger.info("Improve CAP close match by range match:")
        (
            new_close_match
                .where(col("is_full_match") == 1)
                .select("source", "query", "title", "broad_normalized_query", "broad_normalized_title")
                .show(50, False)
        )

        logger.info("Improve CAP close match by synonyms:")
        (
            new_close_match
                .where(col("synonym_expansion.is_full_match_with_synonym") == 1)
                .select("txt_norm_query", "txt_norm_title", "synonym_expansion.expansion", "synonym_expansion.debug")
                .show(50, False)
        )

    # Prepare the output & run NKW rules
    prepare_output(spark, logger, args, new_close_match, janus_nkw, ATv2_CLOSE)
    janus_nkw.unpersist()


if __name__ == "__main__":
    main()