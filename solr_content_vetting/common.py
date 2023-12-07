from pyspark.sql.functions import udf
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, IntegerType, FloatType

from datetime import datetime

s3_working_folder = "s3://solr-content-vetting/"

def s3_path_exists(sc, path):
    # spark is a SparkSession
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
        sc._jsc.hadoopConfiguration(),
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))


# DO NOT USE "%Y/%m/%d" for output folder unless it's your intention
def format_date(date_str, date_format):
    if date_str:
        if "-" in date_str:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        elif "/" in date_str:
            dt = datetime.strptime(date_str, "%Y/%m/%d")
        else:
            dt = datetime.strptime(date_str, "%Y%m%d")
    else:
        dt = datetime.now()

    if date_format:
        return dt.strftime(date_format)

    return dt.strftime("%Y-%m-%d")


'''
Only expand ASINs with non-matched queries
'''
def remove_matched_queries(queries, index):
    if not index:
        return []

    index_tokens = set(index)

    not_matched_queries = []
    for query in queries:
        if not query or not query.normalized_query:
            continue

        tokenized_query_list = query.normalized_query.split(" ")

        if not all(token in index_tokens for token in tokenized_query_list):
            not_matched_queries.append(query)

    return not_matched_queries

remove_matched_queries_UDF = udf(remove_matched_queries, ArrayType(StructType([
    StructField("normalized_query", StringType(), True),
    StructField("query_freq", IntegerType(), True),
    StructField("query_asin_impressions", IntegerType(), True),
    StructField("clicks", IntegerType(), True),
    StructField("adds", IntegerType(), True),
    StructField("purchases", IntegerType(), True),
    StructField("consumes", IntegerType(), True),
])))

'''
Only expand ASINs with non-matched tcs
'''
def remove_matched_tcs(tcs, index):
    if not tcs or not index:
        return []

    index_tokens = set(index)

    not_matched_tcs = []
    for tc in tcs:
        if not tc:
            continue

        tokenized_tc_list = tc.split(" ")

        if not all(token in index_tokens for token in tokenized_tc_list):
            not_matched_tcs.append(tc)

    return not_matched_tcs

remove_matched_tcs_UDF = udf(remove_matched_tcs, ArrayType(StringType()))


def get_query_missing_tokens(queries, index_tokens):
    if not queries:
        return []

    token_set = set(index_tokens)

    expansions = []
    for query in queries:
        if not query.normalized_query:
            continue

        # Get all the missing tokens (in tokenized_query_list, not in index_token)
        missing_tokens = set()
        tokenized_query_list = query.normalized_query.split(" ")
        for token in tokenized_query_list:
            if token not in token_set:
                missing_tokens.add(token)

        # Use missing tokens as expansion
        for missing_token in missing_tokens:
            expansions.append(
                {
                    "expansion": missing_token,
                    "query": query
                }
            )

    return expansions


def get_query_missing_tokens_udf():
    return udf(
        lambda queries, index_tokens: get_query_missing_tokens(queries, index_tokens),
        ArrayType(StructType([
            StructField("expansion", StringType(), True),
            StructField(
                "query",
                StructType(
                    [
                        StructField("normalized_query", StringType(), True),
                        StructField("query_freq", IntegerType(), True),
                        StructField("query_asin_impressions", IntegerType(), True),
                        StructField("clicks", IntegerType(), True),
                        StructField("adds", IntegerType(), True),
                        StructField("purchases", IntegerType(), True),
                        StructField("consumes", IntegerType(), True),
                    ]
                ),
                True
            )
        ]))
    )


def get_tc_missing_tokens(tcs, index_tokens):
    if not tcs:
        return []

    token_set = set(index_tokens)

    expansions = []
    for tc in tcs:
        if not tc:
            continue

        # Get all the missing tokens (in normalized_tc, not in index_token)
        missing_tokens = set()
        normalized_tc_list = tc.split(" ")
        for token in normalized_tc_list:
            if token not in token_set:
                missing_tokens.add(token)

        # Use missing tokens as expansion
        for missing_token in missing_tokens:
            expansions.append(
                {
                    "expansion": missing_token,
                    "tc": tc
                }
            )

    return expansions


def get_tc_missing_tokens_udf():
    return udf(
        lambda tcs, index_tokens: get_tc_missing_tokens(tcs, index_tokens),
        ArrayType(StructType([
            StructField("expansion", StringType(), True),
            StructField("tc", StringType(), True)
        ]))
    )


def remove_bad_expansion(asin, expansion):
    if asin.lower() == expansion.lower():
        return 1

    common_tokens = set(["amazon", "ipad", "prime", "order", "selling", "seller", "world", "your", "my", "2022"])
    color_tokens = set(
        ["white", "black", "red", "pink", "purple", "yellow", "orange", "grey", "gray", "brown", "green", "blue",
         "navy", "golden"])

    if (expansion in common_tokens) or (expansion in color_tokens):
        return 1

    return 0

remove_bad_expansion_UDF = udf(remove_bad_expansion, IntegerType())
