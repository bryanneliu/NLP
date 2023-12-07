import warnings
import logging, sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

from s3_utils import JanusSnapShotReader
from marketplace_utils import (
    get_marketplace_name,
    get_region_realm,
    get_janus_bucket,
    JANUS_PROJECTED_SCHEMA
)

from relax_commons import (
    MT_BROAD,
    get_args, register_tokenization_udf, is_full_match_udf, load_reviewed_synonym,
    synonym_expansion_with_synonym_dict, construct_base_cap, prepare_output
)

from datetime import datetime

warnings.simplefilter(action="ignore", category=FutureWarning)
logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.INFO)

spark = (
    SparkSession.builder.master("yarn")
    .appName("Relax_CAP_broad_match_with_Janus")
    .enableHiveSupport()
    .getOrCreate()
)


def get_most_recent_snapshot_from_bucket(bucket: str, date: str, lookback_hours=9, region="USAmazon") -> str:
    """
        Get the largest Janus snapshot within lookback_hours window. Date has to be in "YYYY/MM/DD" format
    """
    janus_reader = JanusSnapShotReader(bucket, date, region=region, lookback_hours=lookback_hours)
    janus_path = janus_reader.get_largest_snapshot_in_lookback_window()
    logger.info(f"Best Janus snapshot path is {janus_path}")
    return janus_path


def read_JanusS3_as_table(s3_path, verbose_mode) -> str:
    table_name = 'JanusData'
    _ = spark.read.json(s3_path, schema=JANUS_PROJECTED_SCHEMA)
    logger.info("JanusData")

    if verbose_mode:
        _.show(5, False)

    _.createOrReplaceTempView(table_name)
    return table_name


def get_janus_broadmt_df(janus_table, marketplace_id, broadtc_rank_within_asin_threshold, verbose_mode) -> str:
    q = f"""
    WITH t AS (
        SELECT asin, 
               title, 
               adId, 
               advertiserId, 
               campaignId, 
               adGroupId, 
               decorationId, 
               customTextDecorationId, 
               portfolioId,
               businessProgramId,
               isForwardSearchable,
               negativeKeywordListId,
               adGroupNegativeKeywordListId,
               clause
        FROM {janus_table}
        WHERE marketplaceId = {marketplace_id}
              AND asin IS NOT NULL
              AND glProductGroupType NOT IN ('gl_book', 'gl_ebook', 'gl_digital_text', 'gl_digital_ebook_purchase')
              AND businessProgramId IN (3, 6) -- Sponsored Products
              AND decorationId IS NOT NULL
              AND decorationId != 0
              AND clause IS NOT NULL
    )
    SELECT * FROM
    (
        SELECT * FROM t WHERE isForwardSearchable IS NULL
                        AND negativeKeywordListId IS NULL
                        AND adGroupNegativeKeywordListId IS NULL
    ) X
    WHERE size(clause) > 0
    """
    janus_mt = spark.sql(q)

    # dedup the manual targeting clauses
    janus_mt = (
        janus_mt
            .withColumn('clause_info', explode(col('clause')))
            .withColumn('targetingClauseId', col('clause_info').targetingClauseId)
            .withColumn('bid', col('clause_info').bid)
            .withColumn('expression', col('clause_info').expression)
            .withColumn('type', col('clause_info').type)
            .drop(*['clause', 'clause_info'])
    )
    broad_tc = janus_mt.filter(col('type') == MT_BROAD)

    if verbose_mode:
        n_broad_tc = broad_tc.count()
        logger.info(f"Raw Janus Broad type TCID level counts {n_broad_tc}")

    broad_tc.createOrReplaceTempView('broad_tc')
    broad_tc_rank = spark.sql("""
    SELECT *, 
        row_number() over (
    		PARTITION BY asin
    		ORDER BY bid DESC
    	) as rank_within_asin
    FROM (
        SELECT *,
        	row_number() over (
        		PARTITION BY advertiserid,
        		asin,
        		expression,
        		type
        		ORDER BY bid DESC
        	) as rank 
        FROM broad_tc
    ) 
    WHERE rank = 1 
    """)

    broad_tc = (
        broad_tc_rank
            .where(col('rank_within_asin') <= broadtc_rank_within_asin_threshold)
            .drop(*['rank', 'rank_within_asin'])
    )

    if verbose_mode:
        n_broad_tc = broad_tc.count()
        logger.info(f"Janus Broad type TCID, filtered counts {n_broad_tc}")

    return broad_tc


def main():
    args = get_args()
    logger.info(f"Start, arguments are {args}")

    marketplace_name = get_marketplace_name(args.marketplace_id)
    logger.info(f"Janus marketplace name: {marketplace_name}")

    args.tmp_s3_output_path = f'{args.tmp_s3_output_path}/marketplace_id={args.marketplace_id}'
    logger.info(f"tmp_s3_output_path: {args.tmp_s3_output_path}")

    output_path = f"s3://{args.output_bucket}/{args.output_prefix}"
    output_path = f'{output_path}/marketplace_id={args.marketplace_id}'
    logger.info(f"output_path: {output_path}")

    # Load Janus broad match data
    logger.info(f"Start Reading Raw Janus Data")
    date_object = datetime.strptime(args.execute_date, '%Y-%m-%d')
    date_str = date_object.strftime('%Y/%m/%d')
    logger.info(date_str)

    janus_bucket = get_janus_bucket(args.marketplace_id)
    region_realm = get_region_realm(args.marketplace_id)
    marketplace_name = get_marketplace_name(args.marketplace_id)
    logger.info(
        f"Janus bucket {janus_bucket}, Janus region_realm {region_realm}, Janus marketplace name {marketplace_name}")

    s3_path = get_most_recent_snapshot_from_bucket(
        janus_bucket, date_str, lookback_hours=args.janus_lookback_hours, region=region_realm)
    janus_table = read_JanusS3_as_table(s3_path, args.verbose_mode)

    janus_mt_broad_match = get_janus_broadmt_df(janus_table, args.marketplace_id, args.broadtc_rank_within_asin_threshold, args.verbose_mode)
    logger.info(f"Read Janus Broad TC finished, start reading monin CAP and CAP data")

    # Load CAP <query, ASIN, cap_score>
    cap = construct_base_cap(spark, logger, args.marketplace_id, args.region)

    # Join CAP with Janus MT broad match data
    cap_broad_match_candidates = (
        cap.join(
            janus_mt_broad_match,
            "asin",
            "inner"
        )
    )
    cap_broad_match_candidates.createOrReplaceTempView("cap_broad_match_candidates")

    # Tokenize query and targeting clause expression
    register_tokenization_udf(spark)
    candidates_analyzed = (
        spark.sql(
            f"""SELECT *, 
                       txt_norm(searchQuery, '{args.marketplace_id}') AS tokenized_query, 
                       txt_norm(expression, '{args.marketplace_id}') AS tokenized_expression
                FROM cap_broad_match_candidates"""
        )
    )

    # Candidates to relax: col("businessProgramId") == 6 or col("is_full_match") == 0
    candidates_to_relax = (
        candidates_analyzed
            .withColumn(
                "is_full_match",
                is_full_match_udf(col("tokenized_expression"), col("tokenized_query"))
            )
            .where((col("businessProgramId") == 6) | (col("is_full_match") == 0))
    )

    if args.verbose_mode:
        query_ad_count = candidates_to_relax.count()
        logger.info(f"candidates_to_relax query-ad count: {query_ad_count}")

    # Synonym Expansion on query, expression and query full match
    synonym_dict = load_reviewed_synonym(spark, logger)
    synonym_expansion_udf = synonym_expansion_with_synonym_dict(synonym_dict)
    candidates_to_relax = (
        candidates_to_relax
            .withColumn(
                "synonym_expansion",
                synonym_expansion_udf(col("tokenized_expression"), col("tokenized_query"))
            )
    )

    # New broad match candidates
    broad_match_candidates = (
        candidates_to_relax
            .where(
                (col("is_full_match") == 1) |  # col("businessProgramId") == 6
                (col("synonym_expansion.is_full_match_with_synonym") == 1)
                # synonym expansion on query, expression and query full match
            )
    )

    if args.verbose_mode:
        logger.info("Relax CAP broad match by synonyms:")
        (
            broad_match_candidates
                .where(col("synonym_expansion.is_full_match_with_synonym") == 1)
                .select("tokenized_expression", "tokenized_query", "synonym_expansion.expansion", "synonym_expansion.debug")
                .show(50, False)
        )

    # Prepare output
    adtype = "RELAX3"
    horus_dataset = prepare_output(spark, logger, args, adtype, broad_match_candidates)

    # Write final Horus data to S3
    logger.info(f"Start to write ad type: {adtype} final Horus format data")
    horus_dataset.repartition(1000).write.mode("overwrite").json(
        f"{output_path}/{adtype}/locale={marketplace_name.upper()}/"
    )
    logger.info(f"Finished writing ad type: {adtype} final Horus format data")


if __name__ == "__main__":
    main()