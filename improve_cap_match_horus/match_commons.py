import argparse
import collections
import re

from pyspark.sql.types import StringType, ArrayType, IntegerType, StructField, StructType, BooleanType
from pyspark.sql.functions import collect_list, create_map, lit, coalesce, col, udf, when, expr, struct, explode, broadcast, count

from s3_with_nkw_utils import SnapshotMeta

from tokenizer_enhanced import (
    normalize_expressions, normalize_titles
)

ATv2_LOOSE = 'loose-match'
ATv2_CLOSE = 'close-match'
MT_BROAD = 'broad'
MT_EXACT = 'exact'
MT_PHRASE = 'phrase'

SOURCE_CAP = "cap"
SOURCE_TOMMY_ADDITION = "tommy_addition"

'''
Parse args
'''
def get_args():
    parser = argparse.ArgumentParser(description="Improve <query, keyword> Match")
    parser.add_argument(
        "--execute_date",
        type=str,
        default="placeholder_date",
        help="""Execute Date for step functions, in format %Y-%m-%d""",
        required=False,
    )

    parser.add_argument(
        "--marketplace_id",
        type=int,
        default=1,
        help="""Examples: US(1), UK(3)""",
        required=False,
    )

    parser.add_argument(
        "--region",
        type=str,
        default="NA",
        help="""Examples: NA, EU and FE""",
        required=False,
    )

    parser.add_argument(
        "--janus_qdf_bucket",
        type=str,
        default="spear-offline-data-prod",
        help="""Janus AT and MT data""",
        required=False,
    )

    parser.add_argument(
        "--janus_qdf_prefix",
        type=str,
        default="dataset/janus_index_nkw",
        help=""" s3://spear-offline-data-prod/dataset/janus_index_nkw/marketplace=US/date=2023-05-17/hour=00/
                 It has index_docs, janus_mt_data, janus_at_data, janus_nkw_data
             """,
        required=False,
    )

    parser.add_argument(
        "--tmp_output_bucket",
        type=str,
        default="spear-shared",
        help="""Intermediate s3 output bucket""",
        required=False,
    )

    parser.add_argument(
        "--tmp_output_prefix",
        type=str,
        default="dataset/weblab_improve_CAP_match/intermediate",
        help="""Intermediate s3 output prefix path""",
        required=False,
    )

    parser.add_argument(
        "--output_bucket",
        type=str,
        default="spear-shared",
        help="""Output bucket for this job""",
        required=False,
    )

    parser.add_argument(
        "--output_prefix",
        type=str,
        default="dataset/weblab_improve_CAP_match/output",
        help=""" Output prefix for this job """,
        required=False,
    )

    parser.add_argument(
        "--ad_type",
        type=str,
        default="SPEAR3_1",
        help=""" The adType for Horus """,
        required=False,
    )

    parser.add_argument(
        "--verbose_mode",
        type=int,
        default=0,
        help="""Log statistics and samples""",
        required=False,
    )

    parser.add_argument(
        "--rdd_partitions",
        type=int,
        default=0,
        help="""rdd partition number to be optimized""",
        required=False,
    )

    parser.add_argument(
        "--review_count_threshold",
        type=int,
        default=10,
        help="""Head ASINs filtering for OPS optimization""",
        required=False,
    )

    args = parser.parse_args()
    return args


def set_args_paths(args, logger):
    args.tmp_s3_output_path = f"s3://{args.tmp_output_bucket}/{args.tmp_output_prefix}"
    args.tmp_s3_output_path = f'{args.tmp_s3_output_path}/marketplace_id={args.marketplace_id}'
    logger.info(f"tmp_s3_output_path: {args.tmp_s3_output_path}")

    output_path = f"s3://{args.output_bucket}/{args.output_prefix}"
    args.output_path = f'{output_path}/marketplace_id={args.marketplace_id}'
    logger.info(f"output_path: {args.output_path}")

    args.s3_cap_latest = f'{args.tmp_s3_output_path}/cap_latest/'
    args.s3_tommy_addition_latest = f'{args.tmp_s3_output_path}/tommy_addition_latest/'
    args.s3_janus_asins_latest = f'{args.tmp_s3_output_path}/janus_asins_latest/'
    args.s3_targeted_asin_queries = f'{args.tmp_s3_output_path}/targeted_asin_queries_latest'


def register_lexical_matching_udfs(spark):
    """
    Add text norm java udf to spark context for
    subsequent usage in spark sql
    :param spark: spark context
    :return:
    """
    # https://dzone.com/articles/pyspark-java-udf-integration-1
    spark.udf.registerJavaFunction("gen_phrase_keys", "com.amazon.sourcing.keyword.impl.SparkPhraseGenerator", ArrayType(StringType()))
    spark.udf.registerJavaFunction("gen_broad_keys", "com.amazon.sourcing.keyword.impl.SparkBroadKeysGenerator", ArrayType(StringType()))


def register_horus_tokenization_match_udf(spark):
    spark.udf.registerJavaFunction("txt_norm", "com.amazon.sourcing.keyword.impl.SparkKeywordNormalizer", StringType())
    spark.udf.registerJavaFunction("txt_match", "com.amazon.sourcing.keyword.impl.SparkKeywordMatcher", BooleanType())


def get_janus_qdf_data(spark, args, logger, marketplace_name):
    meta = SnapshotMeta()
    janus_prefix = meta.get_snapshot_prefix(
        bucket=args.janus_qdf_bucket,
        meta_prefix=f"{args.janus_qdf_prefix}/marketplace={marketplace_name}/",
    )
    logger.info(f"Obtained Janus prefix from latest snapshot meta at {janus_prefix}")
    logger.info(f"Reading this latest meta ")

    janus_full_path = f"s3://{args.janus_qdf_bucket}/{janus_prefix}"

    janus_mt = spark.read.parquet(f"{janus_full_path}/janus_mt_data/")
    janus_at = spark.read.parquet(f"{janus_full_path}/janus_at_data/")
    janus_nkw = spark.read.parquet(f"{janus_full_path}/janus_nkw_data/")
    janus_nkw = janus_nkw.where(col("keywordlistid").isNotNull())

    return janus_mt, janus_at, janus_nkw


# As each negativeKeywordListId or adGroupNegativeKeywordListId map to a list of negative keywords
# Dedupe logic: keep the top 1 positive, all negatives
def dedupe_expressions(spark, expressions):
    expressions.createOrReplaceTempView('expressions_view')
    tc_rank = spark.sql(f"""
        SELECT *
        FROM (
            SELECT *,
                row_number() over (
                    PARTITION BY advertiserid,
                    asin,
                    expression,
                    type,
                    negativeKeywordListId,
                    adGroupNegativeKeywordListId
                    ORDER BY bid DESC
                ) as rank 
            FROM expressions_view
        ) 
        WHERE rank = 1 
    """)

    tc = tc_rank.drop("rank")
    return tc


def run_negative_targeting(logger, input_table, nkw_data, nkwListId_col: str, marketplace_id: int):
    # filter our [query, nkwlistid] pairs that has violation
    nkw_data = nkw_data.withColumnRenamed("keywordlistid", nkwListId_col)

    # dedup on ['searchQuery', nkwListId_col] level to reduce data volumn for txt_match computation
    logger.info(f'filter by {nkwListId_col}')
    temp = input_table.select(['searchQuery', nkwListId_col]).dropDuplicates()

    # use negative_matched to flag which ['searchQuery', nkwListId_col] pairs are violations
    nkw_violated = (
        temp
            .hint("shuffle_hash") # apply it to the df with duplicated keys
            .join(nkw_data, on=[nkwListId_col])
            .withColumn('negative_matched', expr(f"""exists(negative_clause, x -> txt_match(searchQuery, '{marketplace_id}', x.expression, x.type))"""))
            .where(col('negative_matched'))
            .select(['searchQuery', nkwListId_col])
            .dropDuplicates()
    )

    filtered_after_nkw = (
        input_table
            .hint("shuffle_hash")
            .join(nkw_violated, on=['searchQuery', nkwListId_col], how='leftanti')
    )

    return filtered_after_nkw


'''
all tokens in subset_str need to match in superset_str tokens

Usage:
For close-match, all tokenized query tokens need to match in ASIN tokenized title tokens
is_full_matched(tokenized_query, tokenized_title)

For broad-match, all tokenized expression tokens need to match in tokenized query tokens
is_full_matched(tokenized_expression, tokenized_query)
'''
def is_full_match(subset_str, superset_str):
    if not subset_str or not superset_str:
        return 0

    subset_token_list = subset_str.split(" ")
    superset_token_set = set(superset_str.split(" "))

    is_full_match = all(token in superset_token_set for token in subset_token_list)
    return 1 if is_full_match else 0

is_full_match_udf = udf(is_full_match, IntegerType())


def is_phrase_match(expression, query):
    if not expression or not query:
        return 0

    form1 = expression + " "
    form2 = " " + expression
    form3 = " " + expression + " "

    if (query.startswith(form1)) or (query.endswith(form2)) or (form3 in query):
        return 1

    return 0

is_phrase_match_UDF = udf(is_phrase_match, IntegerType())

'''
For {"MEASUREMENT", "AGE_YEAR", "AGE_MONTH", "SIZE"}, for each range type, generate a list of ranges.
Test cases:
print(extract_ranges(["15/DIMENSION", "30/DIMENSION"]))    
print(extract_ranges(["1/AGE_YEAR_START", "3/AGE_YEAR_END"]))
print(extract_ranges(["3t/SIZE_START", "4t/SIZE_END"]))
print(extract_ranges(["1/AGE_YEAR_START"]))
print(extract_ranges(["1/AGE_YEAR_START", "2t/SIZE_START", "3t/SIZE_END"]))
print(extract_ranges(["1/AGE_YEAR"]))

defaultdict(<class 'list'>, {})
defaultdict(<class 'list'>, {'AGE_YEAR': [(1, 3)]})
defaultdict(<class 'list'>, {'t.SIZE': [(3, 4)]})
defaultdict(<class 'list'>, {'AGE_YEAR': [(1, inf)]})
defaultdict(<class 'list'>, {'AGE_YEAR': [(1, inf)], 't.SIZE': [(2, 3)]})
defaultdict(<class 'list'>, {'AGE_YEAR': [(1, 1)]})
'''
def extract_ranges(token_list):
    ranges = collections.defaultdict(list)
    last_start = None
    current_type = None
    for token in token_list:
        if "/" not in token:
            continue
        # it will return a list containing two elements:
        # the substring before the first "/" and the substring after the first "/".
        word, tag = token.split("/", 1)
        # The rpartition("_") method splits the tag from the rightmost occurrence of "_".
        # The result is a tuple containing three elements: the part before the rightmost "_", the "_", and the part after the rightmost "_".
        # By accessing [-1] on the resulting tuple, we extract the part after the rightmost "_" (interval type).
        # If no "_" is present, the rpartition("_")[-1] will be an empty string. In that case, the expression tag.rpartition("_")[-1] or "START_END" evaluates to "START_END".
        # This ensures that if no interval type is found, the default value "START_END" will be assigned to interval_type.
        # AGE_YEAR_START, interval_type = "START", range_type = "AGE_YEAR"
        interval_type = tag.rpartition("_")[-1] or "START_END"
        range_type = tag.rpartition("_")[0]

        if interval_type not in {"START", "END", "START_END"}:
            interval_type = "START_END"
            range_type = tag

        if range_type not in {"MEASUREMENT", "AGE_YEAR", "AGE_MONTH", "SIZE"}:
            continue

        # To support size range like "3t-4t"
        components = list(filter(None, re.split(r"(\d+(?:[.]\d*)?)", word)))
        if not components:
            continue

        # components should either contain a single number or a number followed by a letter.
        interval_value = float(components[0])
        if len(components) == 2:
            range_type = f"{components[1]}.{range_type}"

        if interval_type == "END":
            ranges[range_type].append((last_start, interval_value))
            last_start = None
            current_type = None
        else:
            # Finish existing interval, if needed.
            if last_start:
                # Must be an unbounded interval, e.g. girls 1+
                ranges[current_type].append((last_start, float('inf')))
                last_start = None
                current_type = None
            if interval_type == "START":
                last_start = interval_value
                current_type = range_type
            else:
                # Must be a single value, e.g. 1 year old girls
                ranges[range_type].append((interval_value, interval_value))
    # Finish existing interval, if needed.
    if last_start:
        # Must be an unbounded interval, e.g. girls 1+
        ranges[current_type].append((last_start, float('inf')))
    return ranges


def extract_dimensions(text):
    tokens = text.split(" ")

    dimensions = collections.defaultdict(int)
    for token in tokens:
        if token.endswith("/DIMENSION"):
            dimensions[token] += 1

    return dimensions


# Support full match for non-measurement tokens
# Support range match for the same measurement:
# query and expression ranges have overlap for the same measurement
def is_range_full_match(expression, query, query_has_num_pattern):
    if not expression or not query:
        return 0

    # Do full match without num patterns
    if not query_has_num_pattern:
        return is_full_match(expression, query)

    # Solve the bad case:
    # query=organic modern rug 8x10: organic/WORD modern/WORD rug/WORD 8/DIMENSION x/IGNORE 10/DIMENSION
    # expression=rug 10x10: rug/WORD 10/DIMENSION x/IGNORE 10/DIMENSION
    if "/DIMENSION" in query:
        query_dimensions = extract_dimensions(query)
        expression_dimensions = extract_dimensions(expression)

        for dimension, count in expression_dimensions.items():
            query_count = query_dimensions[dimension]
            if query_count < count:
            # At least one dimension in the expression does not occur in the query.
                return 0

    # Do range-part match
    query_ranges = extract_ranges(query.split(" "))
    subset_token_list = expression.split(" ")
    expression_ranges = extract_ranges(subset_token_list)

    for range_type, expression_range_values in expression_ranges.items():
        if range_type not in query_ranges:
            return 0
        for expression_range_value in expression_range_values:
            # Check if there is a range in the query that overlaps with this one.
            found_match = False
            for query_range_value in query_ranges[range_type]:
                exp_start, exp_end = expression_range_value
                query_start, query_end = query_range_value
                if not query_end or not exp_start or not exp_end or not query_start:
                    continue
                if query_end < exp_start or exp_end < query_start:
                    continue
                found_match = True
                break
            if not found_match:
                return 0

    # Match the left non-range
    superset_token_set = set(query.split(" "))
    for token in subset_token_list:
        if token in superset_token_set:
            continue

        if "/" in token:
            if token.endswith("/DIMENSION"):
                return 0
            # Range values already checked
            continue

        return 0

    return 1

is_range_full_match_udf = udf(is_range_full_match, IntegerType())


def dump_exact_phrase_samples(args, new_match, match_type):
    samples = (
        new_match
        .select("source", "query", "expression",
                "txt_norm_query", "txt_norm_expression",
                "normalized_query", "normalized_expression")
        .drop_duplicates()
        .limit(50000)
    )
    samples = samples.coalesce(10)
    samples.write.mode("overwrite").csv(
        f"{args.tmp_s3_output_path}/samples/{match_type}/"
    )


def dump_broad_samples(args, new_match, match_type):
    samples = (
        new_match
            .where(col("is_range_full_match") == 1)
            .select("source", "query", "expression", "broad_normalized_query",
                    "broad_normalized_expression", "has_num_pattern")
            .distinct()
            .limit(50000)
    )
    samples = samples.coalesce(10)
    samples.write.mode("overwrite").csv(
        f"{args.tmp_s3_output_path}/samples/{match_type}/range_match/"
    )

    samples = (
        new_match
            .where(col("synonym_expansion.is_full_match_with_synonym") == 1)
            .select("txt_norm_expression", "txt_norm_query",
                    "synonym_expansion.expansion", "synonym_expansion.debug")
            .distinct()
            .limit(2000)
    )
    samples = samples.coalesce(1)
    samples.write.mode("overwrite").parquet(
        f"{args.tmp_s3_output_path}/samples/{match_type}/synonyms/"
    )


def s3_path_exists(sc, path):
    # spark is a SparkSession
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
        sc._jsc.hadoopConfiguration(),
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))


def get_close_match_candidates(spark, args, logger, marketplace_name):
    # asin -> queries
    asin_queries = spark.read.parquet(args.s3_targeted_asin_queries)

    # asin, title
    janus_mt, janus_at, janus_nkw = get_janus_qdf_data(spark, args, logger, marketplace_name)
    close_match = janus_at.where(col("expression") == ATv2_CLOSE)
    asin_title = close_match.select("asin", "title")
    # asin, txt_norm_title, broad_normalized_title
    normalized_asin_title = normalize_titles(spark, args, logger, asin_title)

    match_candidates = (
        asin_queries
            .join(normalized_asin_title, ["asin"], "inner")
            .join(close_match, ["asin"], "inner")
    )

    match_candidates = (
        match_candidates
            .select("*", explode("query_info_list").alias("exploded_query_info"))
            .select(
                "*",
                col("exploded_query_info.query").alias("query"),
                col("exploded_query_info.cap_score").alias("cap_score"),
                col("exploded_query_info.source").alias("source"),
                col("exploded_query_info.txt_norm_query").alias("txt_norm_query"),
                col("exploded_query_info.normalized_query").alias("normalized_query"),
                col("exploded_query_info.broad_normalized_query").alias("broad_normalized_query"),
                col("exploded_query_info.has_num_pattern").alias("has_num_pattern")
            )
            .drop("query_info_list", "exploded_query_info")
    )

    return match_candidates, janus_nkw


def get_match_candidates(spark, args, logger, marketplace_name, mt_type):
    # asin -> queries
    asin_queries = spark.read.parquet(args.s3_targeted_asin_queries)
    asins = asin_queries.select("asin")

    # asin -> ad <expression, txt_norm_expression, normalized_expression>
    janus_mt, janus_at, janus_nkw = get_janus_qdf_data(spark, args, logger, marketplace_name)
    janus_mt_type = janus_mt.where(col("type") == mt_type)
    targeted_tcs = janus_mt_type.join(broadcast(asins), "asin", "inner")
    deduped_tcs = dedupe_expressions(spark, targeted_tcs)
    unique_expressions = deduped_tcs.select("expression").distinct().cache()
    analyzed_expressions = normalize_expressions(
        spark,
        args,
        logger,
        unique_expressions,
        {"broad" if mt_type == MT_BROAD else "default"},
    )
    asin_ad = (
        deduped_tcs
            .hint("shuffle_hash") # apply it to the df with duplicated keys
            .join(analyzed_expressions, ["expression"], "inner")
    )

    match_candidates = (
        asin_queries
            .join(asin_ad.hint("shuffle_hash"), ["asin"], "inner")
    )

    match_candidates = (
        match_candidates
            .select("*", explode("query_info_list").alias("exploded_query_info"))
            .select(
                "*",
                col("exploded_query_info.query").alias("query"),
                col("exploded_query_info.cap_score").alias("cap_score"),
                col("exploded_query_info.source").alias("source"),
                col("exploded_query_info.txt_norm_query").alias("txt_norm_query"),
                col("exploded_query_info.normalized_query").alias("normalized_query"),
                col("exploded_query_info.broad_normalized_query").alias("broad_normalized_query"),
                col("exploded_query_info.has_num_pattern").alias("has_num_pattern")
            )
            .drop("query_info_list", "exploded_query_info")
    )

    if args.verbose_mode:
        logger.info(f"asin_queries count: {asin_queries.count()}")

        targeted_ad_asin_count = deduped_tcs.select("asin").distinct().count()
        logger.info(f"Targeted ad ASIN count: {targeted_ad_asin_count}")

        logger.info(f"asin_ad count: {asin_ad.count()}")

        logger.info(f"Targeted unique_expressions count: {unique_expressions.count()}")

        logger.info(f"match_candidates <query, asin, ad, expression> count: {match_candidates.count()}")

    return match_candidates, janus_nkw


'''
Prepare the Output <query, ASIN, ad, info>
'''
def prepare_output(spark, logger, args, candidates, janus_nkw, match_type):
    candidates.createOrReplaceTempView("candidates_view")
    horus_raw = spark.sql(
    """
        SELECT DISTINCT
            query AS searchQuery,
        	asin,
        	cap_score AS score,
        	targetingClauseId,
            campaignId,
            decorationId,
            adGroupId,
            advertiserId,
            adId,
            negativeKeywordListId,
            adGroupNegativeKeywordListId,
            customTextDecorationId,
            portfolioId,
            businessProgramId,
        	bid,
        	expression,
        	type
        FROM candidates_view
        WHERE (query IS NOT NULL) AND (asin IS NOT NULL) AND (adId IS NOT NULL)
    """)

    # After MT matching we can dedup horus_raw on query, adid level
    # Within an adGroup, adIds share tcIds and negativeKeywordIds, keep the highest bid one
    # AT and MT are in different adGroups
    # Janus is unique adId level
    # adId only exists in 1 adGroupId, so if that adId has nkw list associated, then
    # all ads in that adGroup have that same list
    # adid with nkw and w / o nkw cannot be true at same time
    # the same adId can have multiple tcid, especially for broad match
    if match_type != ATv2_LOOSE:
        horus_raw.createOrReplaceTempView("horus_raw_view")
        horus_raw = spark.sql(
        """
            SELECT 
                *
            FROM
                (
                    SELECT 
                        *,
                        ROW_NUMBER() over (
                            PARTITION BY searchQuery, adid
                            ORDER BY bid DESC
                        ) AS query_adid_rank
                    FROM horus_raw_view
                )
            WHERE query_adid_rank = 1
        """
        )
        horus_raw = horus_raw.drop(*['query_adid_rank'])
    horus_raw.cache()

    if args.verbose_mode:
        query_ad_count = horus_raw.count()
        query_asin_count = horus_raw.select('searchQuery', 'asin').distinct().count()
        logger.info(f"Before NKW filtering, query-ad count: {query_ad_count}, query-ASIN count: {query_asin_count}")

    #
    # Do negative keyword filtering
    # Split candidates_without_nkw and candidates_with_nkw applied run_negative_targeting
    #
    candidates_without_nkw = (
        horus_raw.where((col("negativeKeywordListId").isNull()) &
                        (col("adGroupNegativeKeywordListId").isNull()))
    )

    candidates_with_nkw = (
        horus_raw.where(
            (col("negativeKeywordListId").isNotNull()) |
            (col("adGroupNegativeKeywordListId").isNotNull())
        )
    )

    nkw_filtering_1 = run_negative_targeting(logger, candidates_with_nkw, janus_nkw, 'negativeKeywordListId',
                                                args.marketplace_id)

    nkw_filtering_2 = run_negative_targeting(logger, nkw_filtering_1, janus_nkw, 'adGroupNegativeKeywordListId',
                                                         args.marketplace_id)

    # Join will change column sequence
    # unionAll just merge rows without row deduping, and cause column values wrong
    # Make sure the columns are same before unionAll, or use unionByName
    horus_raw_after_nkw = candidates_without_nkw.unionByName(nkw_filtering_2)

    if args.verbose_mode:
        query_ad_count = horus_raw_after_nkw.count()
        query_asin_count = horus_raw_after_nkw.select('searchQuery', 'asin').distinct().count()
        logger.info(f"After NKW filtering, query-ad count: {query_ad_count}, query-ASIN count: {query_asin_count}")

    horus_raw.unpersist()

    # To improve S3 write
    logger.info("Writing horus_raw_after_nkw to s3")
    # horus_raw_after_nkw = horus_raw_after_nkw.coalesce(1000)
    # horus_raw_after_nkw.repartition(256)
    # horus_raw_after_nkw.write.partitionBy("query_token_count").mode("overwrite").parquet(
    horus_raw_after_nkw.repartition(64).write.mode("overwrite").parquet(
        f"{args.tmp_s3_output_path}/horus_raw_after_nkw/{match_type}/"
    )
    logger.info("Finished writing horus_raw_after_nkw to s3")


# Do query,asin,advertiserid level ad rank
# Do query level ad rank
def filter_output(spark, logger, args, horus_raw_after_nkw):
    horus_raw_after_nkw.createOrReplaceTempView("horus_raw_after_nkw_view")
    horus_data_filtered = spark.sql(
        f"""
            SELECT *
            FROM (
                    SELECT *,
                        ROW_NUMBER() over (
                            PARTITION BY searchQuery
                            ORDER BY score * bid DESC
                        ) AS query_rank,
                        {args.marketplace_id} AS marketplaceId
                        FROM (
                            SELECT *,
                                ROW_NUMBER() over (
                                    PARTITION BY searchQuery, asin, advertiserid
                                    ORDER BY bid DESC
                                ) AS query_asin_rank
                            FROM horus_raw_after_nkw_view
                        )
                    WHERE query_asin_rank = 1
                )
            WHERE query_rank <= 400
        """
    )
    # each adType keep 200 ads in GLO

    query_ad_count = horus_data_filtered.count()
    query_asin_count = horus_data_filtered.select('searchQuery', 'asin').distinct().count()
    unique_query_count = horus_data_filtered.select('searchQuery').distinct().count()
    logger.info(
        f"""After horus_data_filtered, 
            query-ad count: {query_ad_count}, 
            query-ASIN count: {query_asin_count},
            unique_query count: {unique_query_count}
        """
    )

    statistics = horus_data_filtered.groupBy("type").agg(
        count("*").alias("query_ad_count")
    )
    statistics.show(10, False)

    return horus_data_filtered


def generate_json_output(horus_data_filtered, adtype):
    horus_dataset = (
        horus_data_filtered
            .groupBy("searchQuery", "marketplaceId")
            .agg(
                collect_list(
                    create_map(
                        lit("customTextDecorationId"),
                        coalesce(col("customTextDecorationId"), lit("0")),
                        lit("campaignId"),
                        col("campaignId"),
                        lit("decorationId"),
                        col("decorationId"),
                        lit("adGroupId"),
                        col("adGroupId"),
                        lit("advertiserId"),
                        col("advertiserId"),
                        lit("adId"),
                        col("adId"),
                        lit("portfolioId"),
                        col("portfolioId"),
                        lit("businessProgramId"),
                        col("businessProgramId"),
                        lit("asin"),
                        col("asin"),
                        lit("targetingClauseId"),
                        col("targetingclauseid"),
                        lit("bid"),
                        col("bid"),
                        lit("relationshipScore"),
                        lit("0"),
                        lit("numMustMatch"),
                        lit("0"),
                )
            ).alias("ad")
        )
        .withColumn("context", col("searchQuery"))
        .withColumn("searchAlias", lit("aps"))
        .withColumn("offerListingType", lit("PURCHASABLE"))
        .withColumn("relationshipType", lit(adtype))
        .filter(col("searchQuery") != "")
    )

    return horus_dataset


'''
From 2023-03-21 US Janus data:
Janus unique ASIN count: 160134778
Column<'(reviewCount > 0)'>: 23103931, 160134778, 0.1443
Column<'(reviewCount >= 10)'>: 13013433, 160134778, 0.0813
Column<'(reviewCount >= 20)'>: 11018102, 160134778, 0.0688
Column<'(reviewCount >= 50)'>: 8448763, 160134778, 0.05276
Column<'(reviewCount >= 100)'>: 6751534, 160134778, 0.04216
Column<'(reviewCount >= 1000)'>: 2305802, 160134778, 0.0144
Column<'(reviewCount >= 10000)'>: 280992, 160134778, 0.00175
Column<'(reviewCount >= 100000)'>: 5355, 160134778, 3.3441e-05
'''

'''
When joining DataFrame A (with duplicated keys) and DataFrame B (with unique keys), 
the shuffle_hash hint should be applied to the DataFrame with duplicated keys, which is DataFrame A in this case. 
The shuffle_hash hint helps optimize the join performance by redistributing the data using a hash-based shuffle.

.hint("shuffle_hash") # apply it to the df with duplicated keys
'''