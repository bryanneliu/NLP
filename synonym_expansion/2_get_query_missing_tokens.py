import argparse
import logging
import subprocess
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, explode, collect_set, countDistinct, sum, max, collect_list, struct

from expansion_udf import (
    expand_queries_for_synonym_dict
)

from utils import (
    format_date, s3_path_exists, load_jp_synonym,
    get_s3_sp_not_matched_set_prefix
)

from marketplace_utils import (
    get_marketplace_name,
    get_region_name
)

import collections

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
    .appName("get_query_missing_tokens")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext

def process_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--marketplace_id",
        type=int,
        default=1,
        help="Specify solr_index marketplaceid",
        required=True
    )

    parser.add_argument(
        "--solr_index_latest_ds",
        type=str,
        default="2022-12-19",
        help="Specify solr_index ds",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


def load_en_raw_synonym():
    # Synonym format: <src, dest>

    # s3_qu_unigram = "s3://synonym-expansion-experiments/dictionary/en/qu_unigram_tokenized/"
    s3_qu_all = "s3://synonym-expansion-experiments/dictionary/en/qu_all_tokenized/"
    s3_search_config = "s3://synonym-expansion-experiments/dictionary/en/search_config_tokenized/"
    s3_spear_qba_filterd = "s3://synonym-expansion-experiments/dictionary/en/spear_qba_filtered_tokenized/"

    # qu_unigram = spark.read.parquet(s3_qu_unigram)
    qu_all = spark.read.parquet(s3_qu_all)
    search_config = spark.read.parquet(s3_search_config)
    spear_qba_filtered = spark.read.parquet(s3_spear_qba_filterd)

    # Merge synonyms
    synonym_pairs = qu_all.union(search_config).union(spear_qba_filtered)
    logger.info(f"Merged synonym pairs: {synonym_pairs.count()}")

    # Load synonyms pairs to a dictionary - src:list of dest
    synonym_dict = collections.defaultdict(set)
    for row in synonym_pairs.collect():
        src, dest = row[0], row[1]
        synonym_dict[src].add(dest)

    logger.info(f"Synonyms - src count: {len(synonym_dict)}")

    return synonym_dict


def query_based_expansion(args, synonym_dict):
    marketplace_name = get_marketplace_name(args.marketplace_id).lower()
    region = get_region_name(args.marketplace_id).upper()

    solr_index_latest_date = format_date(args.solr_index_latest_ds, "%Y-%m-%d")
    s3_sp_not_matched_set_prefix = get_s3_sp_not_matched_set_prefix(region)

    s3_working_folder = f"{s3_sp_not_matched_set_prefix}{marketplace_name}/solr_index_{solr_index_latest_date}/"
    s3_sp_not_matched_set_folder = f"{s3_working_folder}sp_not_matched_set/"
    s3_missing_token_expansion_folder = f"{s3_working_folder}missing_token_expansion/"
    s3_synonym_expansion_folder = f"{s3_working_folder}synonym_expansion/"

    do_missing_token = True
    expansion_udf = expand_queries_for_synonym_dict(synonym_dict, do_missing_token)

    ls_output = str(subprocess.check_output(['aws', 's3', 'ls', f"{s3_sp_not_matched_set_folder}"]).decode('UTF-8'))
    split_outputs = ls_output.strip().split()
    date_folders = sorted([date_folder for date_folder in split_outputs if 'PRE' not in date_folder])

    for date_folder in date_folders:
        s3_sp_not_matched_set = f"{s3_sp_not_matched_set_folder}{date_folder}"
        s3_missing_token_expansion = f"{s3_missing_token_expansion_folder}{date_folder}"
        s3_synonym_expansion = f"{s3_synonym_expansion_folder}{date_folder}"

        if s3_path_exists(sc, s3_missing_token_expansion):
            continue

        sp_not_matched_with_index = spark.read.parquet(s3_sp_not_matched_set)
        expanded_sp_with_index = (
            sp_not_matched_with_index
                .withColumn(
                    "expansions",
                    expansion_udf(
                        col("not_matched_query_data"),
                        col("title_tokens"),
                        col("index_tokens")
                    )
                )
                .drop("not_matched_query_data", "index_tokens")
                .where(col("expansions") != array())
        )

        expanded_sp = (
            expanded_sp_with_index
                .select("asin", explode(col("expansions")))
                .select("asin", "col.expansion", "col.debug", "col.query")
        )

        missing_token_expansion = expanded_sp.where(col("debug") == "")
        missing_token_expansion_asin_count = missing_token_expansion.select("asin").distinct().count()
        logger.info(f"Start to write missing_tokens to S3: {s3_missing_token_expansion}")
        logger.info(f"missing_token_expansion_asin_count: {missing_token_expansion_asin_count}")
        missing_token_expansion = missing_token_expansion.coalesce(2000)
        missing_token_expansion.write.mode("overwrite").parquet(s3_missing_token_expansion)

        synonym_expansion = expanded_sp.where(col("debug") != "")
        synonym_expansion_asin_count = synonym_expansion.select("asin").distinct().count()
        logger.info(f"Start to write synonym expansion to S3: {s3_synonym_expansion}")
        logger.info(f"synonym_expansion_asin_count: {synonym_expansion_asin_count}")
        synonym_expansion = synonym_expansion.coalesce(2000)
        synonym_expansion.write.mode("overwrite").parquet(s3_synonym_expansion)


def main():
    # Initialization
    args = process_args()

    if args.marketplace_id == 6:
        synonym_dict = load_jp_synonym(spark, logger)
    else:
        synonym_dict = load_en_raw_synonym()

    query_based_expansion(args, synonym_dict)

if __name__ == "__main__":
    main()
