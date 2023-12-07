from pyspark.sql.functions import udf

from pyspark.sql.types import StructField, StructType, StringType, ArrayType, IntegerType, FloatType

import collections

'''
For a matched <tokenized_query, asin, index_tokens>, get query tokens which are not matched in ASIN index_tokens
'''
def get_not_matched_query_tokens(tokenized_query, index_tokens):
    if not tokenized_query or len(tokenized_query) == 0:
        return []

    if not index_tokens or len(index_tokens) == 0:
        return []

    token_set = set(index_tokens)

    # Get all the missing tokens (in tokenized_query_list, not in index_token)
    missing_tokens = set()
    for token in tokenized_query.split(" "):
        if token not in token_set:
            missing_tokens.add(token)

    return list(missing_tokens)

getNotMatchedQueryTokensUDF = udf(get_not_matched_query_tokens, ArrayType(StringType()))


def pt_to_add(expansion, pt):
    if not pt:
        return 0

    pt_tokens = pt.split(" ")
    if (expansion in pt_tokens) or ((expansion + "s") in pt_tokens):
        return 1

    return 0

pt_to_add_UDF = udf(pt_to_add, IntegerType())


def color_to_add(expansion, color):
    if not color:
        return 0

    num_tokens = set(["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "01", "02", "03", "04", "05", "06", "07", "08", "09"])
    color_tokens = color.split(" ")
    if (expansion in color_tokens) and (expansion not in num_tokens):
        return 1

    return 0

color_to_add_UDF = udf(color_to_add, IntegerType())


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
    StructField("min_pos", IntegerType(), True),
    StructField("clicks", IntegerType(), True),
    StructField("adds", IntegerType(), True),
    StructField("purchases", IntegerType(), True),
    StructField("consumes", IntegerType(), True),
])))


def expand_queries(queries, title_tokens, index_tokens, synonym_dict, do_missing_token):
    if not queries:
        return []

    token_set = set(index_tokens)

    expanded_queries = []
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
        if do_missing_token:
            for missing_token in missing_tokens:
                expanded_queries.append(
                    {
                        "expansion": missing_token,
                        "debug": "",
                        "query": query
                    }
                )

        # Do synonym expansion
        if synonym_dict:
            for token in title_tokens:
                dest_set = synonym_dict[token]
                for dest in dest_set:
                    if dest in missing_tokens:
                        expanded_queries.append(
                            {
                                "expansion": dest,
                                "debug": f"{token} :: {dest}",
                                "query": query
                            }
                        )

    return expanded_queries


def expand_queries_for_synonym_dict(synonym_dict, do_missing_token):
    return udf(
        lambda queries, title_tokens, index_tokens: expand_queries(queries, title_tokens, index_tokens, synonym_dict, do_missing_token),
        ArrayType(StructType([
            StructField("expansion", StringType(), True),
            StructField("debug", StringType(), True),
            StructField(
                "query",
                StructType(
                    [
                        StructField("normalized_query", StringType(), True),
                        StructField("query_freq", IntegerType(), True),
                        StructField("query_asin_impressions", IntegerType(), True),
                        StructField("min_pos", IntegerType(), True),
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


def expand_expressions(normalized_expression_str, title_tokens, index_tokens, synonym_dict):
    if not normalized_expression_str or not index_tokens:
        return []

    token_set = set(index_tokens)

    missing_token_count = collections.defaultdict(int)
    for token in normalized_expression_str.split(" "):
        if token not in token_set:
            missing_token_count[token] += 1

    # Do synonym expansion
    res = []
    if synonym_dict:
        for token in title_tokens:
            dest_set = synonym_dict[token]
            for dest in dest_set:
                if (dest in missing_token_count) and (missing_token_count[dest] > 0):
                    res.append(
                        {
                            "expansion": dest,
                            "debug": f"{token} :: {dest}"
                        }
                    )
    return res


def expand_expressions_for_synonym_dict(synonym_dict):
    return udf(
        lambda normalized_expression_str, title_tokens, index_tokens: expand_expressions(normalized_expression_str, title_tokens, index_tokens, synonym_dict),
        ArrayType(StructType([StructField("expansion", StringType(), True), StructField("debug", StringType(), True)]))
    )

