import collections
import unicodedata
import re

from pyspark.sql.types import StringType, ArrayType, IntegerType, StructField, StructType

from pyspark import SparkFiles
from blingfire import load_model, free_model, text_to_words_with_tags_with_model

s3_tag = "s3://personalized-sourcing/ldbsrc/sp_sourcing/tokenizer/wbd.tagset.txt"
s3_bin_file = "s3://personalized-sourcing/ldbsrc/ldb/tokenizer.bin"

'''
Normalize the text "Café" to "Cafe"
'''
def remove_diacritics(text):
    """
    Removes diacritics from text using regular expressions and the unicodedata module.
    Args:
        text (str): The input text with diacritics.
    Returns:
        str: The input text with diacritics removed.
    """
    # Normalize text using NFD normalization form
    normalized_text = unicodedata.normalize('NFD', text)
    # Use regex to remove combining characters (diacritics)
    removed_diacritics_text = re.sub(r'[\u0300-\u036F\u1AB0-\u1AFF\u1DC0-\u1DFF\u20D0-\u20FF\uFE20-\uFE2F]', '',
                                     normalized_text)
    return removed_diacritics_text

# Return (tokenized text, token count)
def analyze_text(tokenizer, text, tag_id_name):
    if not text:
        return "", 0

    try:
        text = text.lower()
        text = remove_diacritics(text)

        # Tokenization
        token_tagids = text_to_words_with_tags_with_model(tokenizer, text)

        if len(token_tagids) != 2:
            return "", 0

        tokenized_text = []
        for (token, tagid) in zip(*token_tagids):
            tag = tag_id_name[str(tagid)]
            if tag == "WORD":
                tokenized_text.append(token)
            elif tag == "MEASUREMENT":
                tokenized_text.append(token + "/MEASUREMENT")
            else:
                tokenized_text.append(tag)

        # return " ".join([str(item) for item in token_tagids[0]])
        # return " ".join([f"{tok}/{tag}/{tag_id_name[tag]}" for tok, tag in zip(*token_tagids)])
        return " ".join(tokenized_text), len(tokenized_text)
    except Exception as e:
        return "", 0


'''
Load lemmatization <token, tagid> to build (tagid -> token) dictionary
tag count: 6659
'''
def load_lemmatization(tag_file):
    tagid_token = collections.defaultdict(str)
    with open(tag_file, 'r') as file:
        # Read each line of the file
        for line in file:
            token_tagid = line[:-1].split(" ") # remove \n at the end of the line
            if len(token_tagid) != 2:
                continue
            tagid_token[token_tagid[1]] = token_tagid[0]
    return tagid_token


def set_spear_tokenizer_resources(spark):
    spark.sparkContext.addFile(s3_tag)
    spark.sparkContext.addFile(s3_bin_file)


def tokenize_query_partition(partitionData):
    # Load the tokenizer
    bin_file = SparkFiles.get("tokenizer.bin")
    tokenizer = load_model(bin_file)

    # Load the tokenizer tag file
    tag_file = SparkFiles.get("wbd.tagset.txt")
    lemma_token_dict = load_lemmatization(tag_file)

    # Run the tokenization
    for row in partitionData:
        if not row or not row.query:
            continue
        tokenized_query, token_count = analyze_text(tokenizer, row.query, lemma_token_dict)
        if not tokenized_query or token_count == 0 or token_count > 20:
            continue
        yield row.query, tokenized_query

    free_model(tokenizer)


def tokenize_expression_partition(partitionData):
    # Load the tokenizer
    bin_file = SparkFiles.get("tokenizer.bin")
    tokenizer = load_model(bin_file)

    # Load the tokenizer tag file
    tag_file = SparkFiles.get("wbd.tagset.txt")
    lemma_token_dict = load_lemmatization(tag_file)

    # Run the tokenization
    for row in partitionData:
        if (not row) or (not row.expression):
            continue
        tokenized_expression, token_count = analyze_text(tokenizer, row.expression, lemma_token_dict)
        if (not tokenized_expression) or (token_count == 0) or (token_count > 20):
            continue
        yield row.expression, tokenized_expression

    free_model(tokenizer)


'''
1.  Convert a DataFrame to an RDD using the rdd attribute
2.  Spark typically sets the number of partitions based on the default configuration property spark.sql.shuffle.partitions. 
    This property determines the default number of partitions used for shuffling data during certain operations, such as joins or aggregations.
    4000 in the current setting.
3.  Repartition using rdd.repartition(desired_partitions)
    As each partition will load a tokenizer, too many partitions can cause out of memory issue.
    We experiment desired_partitions to balance memory and performance.
'''
def rdd_repartition(logger, df, desired_partitions):
    # Convert DataFrame to RDD
    rdd = df.rdd

    # Get the number of partitions in the RDD
    num_partitions = rdd.getNumPartitions()
    logger.info(f"Default num of partitions in the RDD: {num_partitions}")

    # Repartition the RDD into a desired number of partitions
    evenly_distributed_rdd = rdd.repartition(desired_partitions)

    # Check the number of partitions in the evenly distributed RDD
    num_partitions = evenly_distributed_rdd.getNumPartitions()
    logger.info(f"Evenly distributed num of partitions in the RDD: {num_partitions}")

    return evenly_distributed_rdd


'''
Normalize CAP query to get old and new tokenized queries
'''
def normalize_queries(spark, args, logger, cap):
    unique_queries = cap.select("query").distinct().cache()
    unique_queries.createOrReplaceTempView("unique_queries_view")
    txt_norm_queries = (
        spark.sql(f"SELECT query, txt_norm(query, '{args.marketplace_id}') AS txt_norm_query FROM unique_queries_view")
    )

    desired_partitions = args.rdd_partitions
    evenly_distributed_rdd = rdd_repartition(logger, unique_queries, desired_partitions)
    normalized_queries = evenly_distributed_rdd.mapPartitions(tokenize_query_partition)

    # Define the schema for the DataFrame
    schema = StructType([
        StructField('query', StringType(), nullable=False),
        StructField('normalized_query', StringType(), nullable=False)
    ])

    # Create the DataFrame to ensure compatibility.
    # rdd.toDF can cause error when the result has compatibility issue
    normalized_queries_df = spark.createDataFrame(normalized_queries, schema)

    normalized_cap = (
        cap
            .join(txt_norm_queries.hint("shuffle_hash"), ["query"], "inner")
            .join(normalized_queries_df.hint("shuffle_hash"), ["query"], "inner")
    )

    if args.verbose_mode:
        logger.info("normalized_cap samples:")
        samples = normalized_cap.select("query", "txt_norm_query", "normalized_query").distinct()
        samples.show(50, False)

    return normalized_cap


'''
Normalize expressions to get old and new tokenized expressions
For dataframe join:
  * Avoid big join small join small
  * Do small join small join big
  * Use unique_expression instead of full ad to first join txt_norm_expression, 
    then join normalized_expression
'''
def normalize_expressions(spark, args, logger, unique_expressions):
    unique_expressions.createOrReplaceTempView("unique_expressions_view")
    txt_norm_expressions = (
        spark.sql(f"SELECT expression, txt_norm(expression, '{args.marketplace_id}') AS txt_norm_expression FROM unique_expressions_view")
    )

    desired_partitions = args.rdd_partitions
    evenly_distributed_rdd = rdd_repartition(logger, unique_expressions, desired_partitions)
    normalized_expressions = evenly_distributed_rdd.mapPartitions(tokenize_expression_partition)

    # Define the schema for the DataFrame
    schema = StructType([
        StructField('expression', StringType(), nullable=False),
        StructField('normalized_expression', StringType(), nullable=False)
    ])

    # Create the DataFrame to ensure compatibility.
    # rdd.toDF can cause error when the result has compatibility issue
    normalized_expressions_df = spark.createDataFrame(normalized_expressions, schema)

    analyzed_expressions = (
        unique_expressions
            .join(txt_norm_expressions, ["expression"], "inner")
            .join(normalized_expressions_df, ["expression"], "inner")
    )

    return analyzed_expressions
