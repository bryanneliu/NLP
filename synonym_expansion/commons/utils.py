import collections

from datetime import datetime

def get_s3_match_set_prefix(region):
    case_dict = {
        "NA": "s3://synonym-expansion-experiments/asin_level_synonym_expansion/",
        "FE": "s3://spear-tommy-top-impressions/",
        "EU": "s3://spear-tommy-top-impressions/"
    }
    return case_dict.get(region, "")


def get_s3_sp_not_matched_set_prefix(region):
    s3_prefix = "s3://synonym-expansion-experiments/"
    return f"{s3_prefix}{region}/"


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


# It's one-way synonym
def load_jp_synonym(spark, logger):
    # Synonym format: <src, dest>
    synonym_pairs = spark.read.option("sep", "\t").option("header", True).csv("s3://synonym-expansion-experiments/dictionary/jp/hq_jp_synonym_pairs.csv")
    logger.info(f"Merged synonym pairs: {synonym_pairs.count()}")

    # Load synonyms pairs to a dictionary - src:list of dest
    synonym_dict = collections.defaultdict(set)
    for row in synonym_pairs.collect():
        src, dest = row[0], row[1]
        synonym_dict[src].add(dest)

    logger.info(f"Synonyms - src count: {len(synonym_dict)}")
    return synonym_dict



def load_reviewed_synonym(spark, logger):
    # Synonym format: <src, dest>

    reviewed_synonym = spark.read.options(header='True', delimiter='\t').csv(
        "s3://synonym-expansion-experiments/NA/synonym/reviewed_synonym/")
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
