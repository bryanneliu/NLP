import warnings
import logging, sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from marketplace_utils import get_marketplace_name

from tokenizer_enhanced import (
    set_spear_tokenizer_resources
)

from synonym import (
    load_reviewed_synonym, synonym_expansion_with_synonym_dict
)

from match_commons import (
    MT_BROAD, SOURCE_CAP, SOURCE_TOMMY_ADDITION,
    get_args, set_args_paths, register_horus_tokenization_match_udf,
    is_full_match_udf, is_range_full_match_udf,
    get_match_candidates, dump_broad_samples, prepare_output
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
    .appName("Improve_CAP_broad_match")
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

    # Get broad match candidates
    broad_match_candidates, janus_nkw = get_match_candidates(spark, args, logger, marketplace_name, MT_BROAD)
    janus_nkw.cache()

    broad_match_candidates = (
        broad_match_candidates
            .withColumn(
                "is_txt_norm_full_match",
                is_full_match_udf(col("txt_norm_expression"), col("txt_norm_query"))
            )
    )

    # Synonym Expansion on query, expression and query full match
    synonym_dict = load_reviewed_synonym(spark, logger)
    synonym_expansion_udf = synonym_expansion_with_synonym_dict(synonym_dict)

    # Only apply the improvements to non-prod cap broad match
    candidates_to_improve = (
        broad_match_candidates
            .where(
                ((col("source") == SOURCE_CAP) & (col("is_txt_norm_full_match") == 0)) |
                (col("source") == SOURCE_TOMMY_ADDITION)
            )
            .withColumn(
                "is_range_full_match",
                is_range_full_match_udf(col("broad_normalized_expression"), col("broad_normalized_query"), col("has_num_pattern"))
            )
            .withColumn(
                "synonym_expansion",
                synonym_expansion_udf(col("txt_norm_expression"), col("txt_norm_query"))
            )
    )

    # Get new broad matches
    new_broad_match = (
        candidates_to_improve
            .where(
                (col("is_txt_norm_full_match") == 1) |
                (col("is_range_full_match") == 1) |
                (col("synonym_expansion.is_full_match_with_synonym") == 1)
            )
    )

    if args.verbose_mode:
        dump_broad_samples(args, new_broad_match, MT_BROAD)

        cap_new_broad_match = new_broad_match.where(col("source") == SOURCE_CAP)
        tommy_addition_new_broad_match = new_broad_match.where(col("source") == SOURCE_TOMMY_ADDITION)
        logger.info(f"cap_new_broad_match count: {cap_new_broad_match.count()}")
        logger.info(f"tommy_addition_new_broad_match count: {tommy_addition_new_broad_match.count()}")

    # Prepare output & run NKW rules
    prepare_output(spark, logger, args, new_broad_match, janus_nkw, MT_BROAD)

    janus_nkw.unpersist()

if __name__ == "__main__":
    main()

'''
**** Performance Improvements ****

Debug version: 3 hours 52 minutes (230 minutes)
1. 15:36 to 15:41:16 -> broad_match_candidates: 5 minutes
2. 15:41:16 to 16:35:29 -> tommy_addition_new_broad_match count (55 minutes for synonym, match udf)
3. 16:35:29 to 16:58:30 -> show synonym (23 minutes for synonym distinct and show)
4. 16:58:30 to 18:10:34 -> query,adid level dedupe, before NKW filtering (70 minutes)
5. 18:10:34 to 19:27:48 -> after NKW filtering (77 minutes for nkw filtering)
5 + 55 + 23 + 70 + 77 = (230 minutes)

Non-debug version: 2 hours 8 minutes (128 minutes)
19:24:43 -> 19:25:26: synonym dict
19:25:26 -> 20:13:16: Before NKW filtering (48 minutes for synonym, match udf, <query,adid> dedupe)
20:13:16 -> 21:31:55: After NKW filtering (78 minutes for nkw filtering)
21:31:55 -> 21:32:16: After horus_data_filtered

get_match_candidates was a bottleneck:
1. Avoid big dataframe join: split dataframe
2. Pay attention to dataframe join sequence: join small dataframes first
3. Rows are more expansive than the columns, collect_list, then explode
   columns are cheap, rows are expensive
4. Only select the necessary columns for join, try to apply broadcast for join
   Cannot broadcast the table that is larger than 8GB and also more than 100M rows?

nkw filtering is a bottleneck: 
1. (positive + negative matches) -> ['searchQuery', nkwListId_col] pairs are violations
2. (positive + negative matches) leftanti join (['searchQuery', nkwListId_col] pairs are violations)
=> separate positive and negative matches candidates, only use negative matches to check violations

Support verbose_mode to disable statistics in prod
'''

