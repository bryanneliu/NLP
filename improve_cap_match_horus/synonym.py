from pyspark.sql.types import StringType, ArrayType, IntegerType, StructField, StructType
from pyspark.sql.functions import collect_list, create_map, lit, coalesce, col, udf, when
import collections

s3_reviewed_synonym = "s3://synonym-expansion-experiments/NA/synonym/reviewed_synonym/"

def load_reviewed_synonym(spark, logger):
    # Synonym format: <src, dest>
    reviewed_synonym = spark.read.options(header='True', delimiter='\t').csv(s3_reviewed_synonym)
    logger.info(f"Reviewed synonym pairs: {reviewed_synonym.count()}")

    '''
    label:0, not synonym
    label:1, 1-way synonym
    label:2, 2-way synonym
    +---------------------+-----+
    |pair                 |label|
    +---------------------+-----+
    |accessory :: accesory|1    |
    |woman :: lady        |2    |
    |pulley :: pully      |1    |
    |massager :: massger  |1    |
    |bluetooth :: bluetoot|1    |
    |sack :: sac          |0    |
    |anchor :: ancor      |1    |
    +---------------------+-----+
    '''

    # Load synonyms pairs to a dictionary - src:list of dest
    synonym_dict = collections.defaultdict(set)
    for row in reviewed_synonym.collect():
        pair, label = row[0], row[1]
        parts = pair.split(" :: ")
        if len(parts) != 2 or label == "0":
            continue

        src, dest = parts[0], parts[1]

        if label == "1":
            synonym_dict[src].add(dest)
        elif label == "2":
            synonym_dict[src].add(dest)
            synonym_dict[dest].add(src)

    logger.info(f"Synonyms - src count: {len(synonym_dict)}")

    pair_count = 0
    for src, dest_set in synonym_dict.items():
        pair_count += len(dest_set)

    logger.info(f"Synonyms - pair count: {pair_count}")

    return synonym_dict

'''
Using high precision synonyms to support close and broad match.
Synonym expansion for close-match.
close-match: all tokenized query tokens match in tokenized title tokens
# query missing token = "trainer", title = "shoe" -> do not expand
# query missing token = "shoe", title = "trainer" -> expand
# synonym expansion on title
'''
def synonym_expansion_with_debug(tokenized_query, tokenized_title, synonym_dict):
    if not tokenized_query or not tokenized_title:
        return {"is_full_match_with_synonym": 0, "expansion": [], "debug": []}

    token_set = set(tokenized_title.split(" "))

    # Get all the missing tokens in tokenized_query_list
    missing_tokens = set()
    for token in tokenized_query.split(" "):
        if token not in token_set:
            missing_tokens.add(token)

    if not missing_tokens:
        return {"is_full_match_with_synonym": 0, "expansion": [], "debug": []}

    expansion = set()
    debug = set()

    # Uni-gram synonym expansion
    for token in token_set:
        dest_set = synonym_dict[token]
        for dest in dest_set:
            if dest in missing_tokens:
                missing_tokens.remove(dest)
                expansion.add(dest)
                debug.add(token + " :: " + dest)

    if not missing_tokens:
        return {"is_full_match_with_synonym": 1, "expansion": list(expansion), "debug": list(debug)}
    else:
        return {"is_full_match_with_synonym": 0, "expansion": [], "debug": []}


def synonym_expansion_with_synonym_dict(synonym_dict):
    return udf(
        lambda tokenized_query, tokenized_title: synonym_expansion_with_debug(tokenized_query, tokenized_title, synonym_dict),
        StructType([
            StructField("is_full_match_with_synonym", IntegerType()),
            StructField("expansion", ArrayType(StringType())),
            StructField("debug", ArrayType(StringType()))
        ])
    )