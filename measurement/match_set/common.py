from pyspark.sql.functions import udf, when
from pyspark.sql.types import IntegerType, StringType, ArrayType

from datetime import datetime

# Constants
s3_Tommy_top_impressions_prefix = "s3://spear-tommy-top-impressions/solr8/"
s3_match_set_prefix = "s3://match-set/"
s3_solr_index_prefix = "s3://spear-shared/dataset/solr_index/ds="
s3_match_set_prefix_for_solrTPS = "s3://spear-team/solrTPS/"


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


def s3_path_exists(sc, path):
    # spark is a SparkSession
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
        sc._jsc.hadoopConfiguration(),
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))


'''
User defined functions - UDF
'''
def get_query_token_count(query):
    if not query:
        return 0
    return len(query.split(" "))

get_query_token_count_UDF = udf(get_query_token_count, IntegerType())


# Add columns is_title_matched, is_index_matched in the match set
def is_matched(tokenized_query, tokenized_index):
    if not tokenized_query or not tokenized_index:
        return 0

    tokenized_query_list = tokenized_query.split(" ")
    tokenized_index_set = set(tokenized_index)

    is_matched = all(token in tokenized_index_set for token in tokenized_query_list)
    return 1 if is_matched else 0

is_matched_UDF = udf(lambda tokenized_query, tokenized_index: is_matched(tokenized_query, tokenized_index), IntegerType())


# Add columns is_matched_with_expansion
def is_matched_with_expansion(tokenized_query, tokenized_index, new_dataset):
    if not tokenized_query or not tokenized_index:
        return 0

    tokenized_query_list = tokenized_query.split(" ")
    tokenized_index_set = set(tokenized_index)

    if new_dataset:
        for token in new_dataset.split(" "):
            tokenized_index_set.add(token)

    is_matched = all(token in tokenized_index_set for token in tokenized_query_list)

    return 1 if is_matched else 0

is_matched_with_expansion_UDF = udf(is_matched_with_expansion, IntegerType())


# Add columns extra_tokens
def generate_extra_tokens(tokenized_index, new_dataset):
    ''' new_dataset is the new index field, after solr tokenizer normalization.
    '''
    tokenized_index_set = set(tokenized_index)
    extra = []
    if not new_dataset:
        return extra
    new_data_set = set(new_dataset.split(" "))

    extra = list(new_data_set.difference(tokenized_index_set))

    return extra
generate_extra_tokens_UDF = udf(generate_extra_tokens, ArrayType(StringType()))


# Add column is_index_bmm_matched
def is_matched_with_bmm(tokenized_query, tokenized_index):
    ''' use BMM 1223 rule for matching on given field. 
    '''
    if not tokenized_query or not tokenized_index:
        return 0

    tokenized_query_list = tokenized_query.split(" ")
    tokenized_index_set = set(tokenized_index)

    n_query_tok = len(tokenized_query_list)
    if n_query_tok <= 2:
        is_matched = all(token in tokenized_index_set for token in tokenized_query_list)
    elif n_query_tok == 3:
        is_matched = (sum(token in tokenized_index_set for token in tokenized_query_list) >= 2)
    else:
        is_matched = (sum(token in tokenized_index_set for token in tokenized_query_list) >= 3)
    
    return 1 if is_matched else 0

is_matched_with_bmm_UDF = udf(is_matched_with_bmm, IntegerType())