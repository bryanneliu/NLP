import warnings
import logging, sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from marketplace_utils import get_marketplace_name

from tokenizer_enhanced import (
    set_spear_tokenizer_resources
)

from match_commons import (
    MT_EXACT, SOURCE_CAP, SOURCE_TOMMY_ADDITION,
    get_args, set_args_paths, register_horus_tokenization_match_udf,
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
    .appName("Improve_CAP_exact_match")
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

    # Get exact match candidates
    exact_match_candidates, janus_nkw = get_match_candidates(spark, args, logger, marketplace_name, MT_EXACT)
    janus_nkw.cache()

    # Get new exact matches
    new_exact_match = (
        exact_match_candidates
            .where(
                (
                    (col("source") == SOURCE_CAP) &
                    (col("txt_norm_query") != col("txt_norm_expression")) &
                    (col("normalized_query") == col("normalized_expression"))
                ) |
                (
                    (col("source") == SOURCE_TOMMY_ADDITION) &
                    (
                        (col("txt_norm_query") == col("txt_norm_expression")) |
                        (col("normalized_query") == col("normalized_expression"))
                    )
                )
            )
    ).cache()

    if args.verbose_mode:
        dump_exact_phrase_samples(args, new_exact_match, MT_EXACT)

        cap_new_exact_match = new_exact_match.where(col("source") == SOURCE_CAP)
        tommy_addition_new_exact_match = new_exact_match.where(col("source") == SOURCE_TOMMY_ADDITION)
        logger.info(f"cap_new_exact_match count: {cap_new_exact_match.count()}")
        logger.info(f"tommy_addition_new_exact_match count: {tommy_addition_new_exact_match.count()}")


    # Prepare the output & run NKW rules
    prepare_output(spark, logger, args, new_exact_match, janus_nkw, MT_EXACT)
    new_exact_match.unpersist()
    janus_nkw.unpersist()

if __name__ == "__main__":
    main()

'''
When to use .cache():
1. If you plan to reuse a DataFrame multiple times in your code, caching it can prevent recomputation. 
2. Small to medium-sized DataFrames.
3. Remember that caching DataFrames consumes memory, and you should consider the available resources in your cluster. 
   Additionally, you can use the .unpersist() method to release the DataFrame from memory when it is no longer needed.
* Only apply to the dataframes which are used multi-times later
* Call .cache() in the high level code, call .unpersist() to release the df from the memory
* Within a function, do not call .unpersist()
  
repartition(numPartitions) allows you to specify the desired number of partitions for the DataFrame. It performs a full shuffle of the data and redistributes it across the specified number of partitions.
coalesce(numPartitions) is a more efficient operation compared to repartition(). It can decrease the number of partitions by combining them, but it does not perform a full shuffle.
'''
