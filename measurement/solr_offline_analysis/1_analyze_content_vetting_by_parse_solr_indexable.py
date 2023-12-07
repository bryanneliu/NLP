import argparse
import logging
import sys

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, size, explode, count, sum, udf, split, countDistinct, max, lit, when, avg
import pyspark.sql.functions as f

from schemas import *

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
    .appName("analyze_content_vetting")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext


def creat_vetId_fieldName_df():
    vetId_fieldName = (
        spark.createDataFrame(
            vetId_fieldName_mapping, schema=vetId_fieldName_schema
        )
    )
    return vetId_fieldName


def load_indexable_mt(s3_indexable, marketplace_id):
    logger.info(f"s3_indexable: {s3_indexable}")
    indexable = spark.read.format('json').load(s3_indexable, schema=INDEXABLE_PROJECTED_SCHEMA)
    indexable = indexable.where(col("marketplaceId") == f'{marketplace_id}')

    indexable = indexable.withColumn("ad_count", size(col("ad")))

    # at and mt are in different adGroups
    indexable_at = indexable.where(f.col("isForwardSearchable").isNotNull())
    indexable_mt = indexable.where(f.col("isForwardSearchable").isNull())
    # To get MT adGroup statistics, below 2 ways are equivalent
    # 1. Use isForwardSearchable to filter out AT adGroups
    # 2. Use the full indexable, exclude all AT tcs <adGroup_mt, tcs>, join with <adGroup, ads>

    '''
    For Debug
    '''
    logger.info("For debug:")
    indexable.select("child").printSchema()
    indexable_at.select("child").show(2, False)
    # indexable_mt.select("child").show(2, False)

    adGroup_count = indexable.select("adGroupId").distinct().count()
    at_adGroup_count = indexable_at.select("adGroupId").count()
    mt_adGroup_count = indexable_mt.select("adGroupId").count()
    logger.info(f"adGroup_count: {adGroup_count}, at_adGroup_count: {at_adGroup_count}, mt_adGroup_count: {mt_adGroup_count}")

    max_ad_count_at = indexable_at.select(max(col("ad_count").alias("at_max_ad_count")))
    max_ad_count_mt = indexable_mt.select(max(col("ad_count").alias("mt_max_ad_count")))
    max_ad_count_at.show(2, False)
    max_ad_count_mt.show(2, False)

    return indexable_mt


def get_exploded_ad(indexable):
    exploded_ad = (
        indexable
            .select("adGroupId", explode("ad"))
            .select("adGroupId", "col.adId", "col.asin", "col.glProductGroupType")
    )

    ad = exploded_ad.select("adGroupId", "adId").distinct()
    asin_ad = exploded_ad.select("adGroupId", "adId", "asin").distinct()

    book_ad = (
        exploded_ad
            .where(col("glProductGroupType").isin(glProductGroupType_book))
            .select("adGroupId", "adId").distinct()
    )

    non_book_ad = (
        exploded_ad
            .where(~col("glProductGroupType").isin(glProductGroupType_book))
            .select("adGroupId", "adId").distinct()
    )

    logger.info(f"ad count: {ad.count()}, book ad count: {book_ad.count()}, non_book ad count: {non_book_ad.count()}")

    '''
    For debug
    '''
    logger.info("ASIN with more than one adId or adGroupId:")
    check = (
        asin_ad.groupBy("asin").agg(
            countDistinct("adGroupId").alias("distinct_adGroupId_count"),
            countDistinct("adId").alias("distinct_adId_count")
        )
    )
    check.sort(col("distinct_adId_count").desc()).show(5, False)
    check.sort(col("distinct_adGroupId_count").desc()).show(5, False)

    return ad, asin_ad, book_ad, non_book_ad


'''
Keep MT types: exact, phrase, broad
Return adGroupId, targetingClauseId, expression, type, adId_vetId, expr_token_count
'''
def get_exploded_tc(indexable):
    exploded_tc = (
        indexable
            .select( # explode child to get individual tc
                "adGroupId",
                "ad_count",
                explode(col("child")).alias("tc")
            )
            .select(
                "adGroupId",
                "ad_count",
                "tc.targetingClauseId",
                "tc.expression",
                "tc.type",
                "tc.adId_vetId"
            )
            .where( # remove AT whose adId_vetId = null
                (col("type") != "TARGETING_EXPRESSION_PREDEFINED") &
                (col("type") != "auto")
            ) # remove the duplicated tc
            .where(col("expression") != "")  # remove the duplicates
            .withColumn("expr_token_count", size(split(col("expression"), " ")))
    )

    return exploded_tc


'''
# adId_vetId are string or array, we load it as string and parse it to array
# adId_vetId: ["200144034138098,2","200144034138198,2"]
# adId_vetId: 200144034138098,2
'''
def get_exploded_vetId(exploded_tc):
    parsed_tc = exploded_tc.withColumn("parsedVetId", parseVetId_udf(col("adId_vetId")))

    exploded_vetId = (
        parsed_tc
            .where(col("adId_vetId").isNotNull())
            .select(
                "adGroupId",
                "targetingClauseId",
                "expression",
                "type",
                "expr_token_count",
                explode("parsedVetId").alias("adId_vetId")
            )
            .select(
                "adGroupId",
                "targetingClauseId",
                "expression",
                "type",
                "expr_token_count",
                split(col("adId_vetId"), ",").getItem(0).alias("adId"),
                split(col("adId_vetId"), ",").getItem(1).alias("vetId")
            )
    )

    # logger.info("exploded_vetId:")
    # exploded_vetId.show(5, False)

    logger.info("Check whether <tcId, adId> can have multiple vetId")
    check = (
        exploded_vetId
            .groupBy("targetingClauseId", "adId")
            .agg(count("vetId").alias("vetId_count"))
    )
    check.where(col("vetId_count") > 1).show(10, False)

    return exploded_vetId


# updated_vetId: adGroupId, targetingClauseId, type, expr_token_count, adId, vetId
# adGroup_statistics: adGroupId, ad_count, tc_count, exact_tc_count, phrase_tc_count, broad_tc_count
#    tc_ad_count, exact_tc_ad_count, phrase_tc_ad_count, broad_tc_ad_count
def get_vetId_distribution(updated_vetId, vetId_fieldName):
    updated_vetId = updated_vetId.select("targetingClauseId", "adId", "vetId").distinct()

    vetId_statistics = (
        updated_vetId
            .groupBy("vetId").agg(
                countDistinct("targetingClauseId").alias("tc_count"),
                count("*").alias("tc_ad_count")
            )
    )
    # For the tc calculation, simulate the prod implementation
    # tcDocument.addField(FieldConstants.VET_ID, vetId);
    # In Prod, tcId is vetted to at least 1 ad by vetId at least once, it will be counted once
    total_vetted_tc_count = vetId_statistics.select(sum(col("tc_count"))).collect()[0][0]
    total_vetted_tc_ad_count = updated_vetId.select("targetingClauseId", "adId").distinct().count()

    vetId_statistics = (
        vetId_statistics
            .withColumn("tc_percentage", f.round(col("tc_count")/total_vetted_tc_count, 4))
            .withColumn("tc_ad_percentage", f.round(col("tc_ad_count") / total_vetted_tc_ad_count, 4))
            .join(vetId_fieldName, ["vetId"], "inner")
    )

    logger.info("VetId distribution:")
    (
        vetId_statistics
            .select("vetId", "tc_percentage", "tc_ad_percentage", "fieldName", "tc_count", "tc_ad_count")
            .sort(f.col("tc_ad_percentage").desc())
            .show(50, False)
    )


def get_null_vetId_statistics(exploded_tc, asin_ad):
    logger.info("tc with null adId_vetId samples for each type:")

    null_vetId = exploded_tc.where(col("adId_vetId").isNull())
    null_vetId_with_asin = null_vetId.join(asin_ad, ["adGroupId"], "inner")

    null_vetId_with_asin.where(col("type") == "exact").show(50, False)
    null_vetId_with_asin.where(col("type") == "phrase").show(50, False)
    null_vetId_with_asin.where(col("type") == "broad").show(50, False)


def main():
    # Initialization
    args = process_args()

    if args.task.lower() != "content_vetting":
        return

    vetId_fieldName = creat_vetId_fieldName_df()

    # Load indexable: for the content vetting analysis, we focus on MT adGroups
    indexable = load_indexable_mt(args.s3_indexable, args.marketplace_id)

    # Explode adGroup ad array to get: adGroupId, adId
    exploded_ad, asin_ad, book_ad, non_book_ad = get_exploded_ad(indexable)

    # Explode adGroupId tc array to get:
    # adGroupId, ad_count, targetingClauseId, expression, type, adId_vetId, expr_token_count
    # Remove duplicated & AT tcs
    exploded_tc = get_exploded_tc(indexable)

    # adGroupId, ad_count, tc_count, exact_tc_count, phrase_tc_count, broad_tc_count
    # tc_ad_count, exact_tc_ad_count, phrase_tc_ad_count, broad_tc_ad_count
    adGroup_statistics = (
        exploded_tc
            .groupBy("adGroupId", "ad_count").agg(
                count("targetingClauseId").alias("tc_count"),
                sum(when((col("type") == "exact"), 1).otherwise(0)).alias("exact_tc_count"),
                sum(when((col("type") == "phrase"), 1).otherwise(0)).alias("phrase_tc_count"),
                sum(when((col("type") == "broad"), 1).otherwise(0)).alias("broad_tc_count"),
            )
            .withColumn("tc_ad_count", col("tc_count")*col("ad_count"))
            .withColumn("exact_tc_ad_count", col("exact_tc_count")*col("ad_count"))
            .withColumn("phrase_tc_ad_count", col("phrase_tc_count")*col("ad_count"))
            .withColumn("broad_tc_ad_count", col("broad_tc_count")*col("ad_count"))
    )

    logger.info("MT adGroup max tc, ad, tc_ad info:")
    adGroup_max_info = (
        adGroup_statistics.select(
            max(col("tc_count")).alias("max_tc_count"),
            max(col("ad_count")).alias("max_ad_count"),
            max(col("tc_ad_count")).alias("max_tc_ad_count"),
            max(col("exact_tc_count")).alias("max_exact_tc_count"),
            max(col("phrase_tc_count")).alias("max_phrase_tc_count"),
            max(col("broad_tc_count")).alias("max_broad_tc_count"),
            max(col("exact_tc_ad_count")).alias("max_exact_tc_ad_count"),
            max(col("phrase_tc_ad_count")).alias("max_phrase_tc_ad_count"),
            max(col("broad_tc_ad_count")).alias("max_broad_tc_ad_count"),
        )
    )
    adGroup_max_info.show(5, False)

    get_null_vetId_statistics(exploded_tc, asin_ad)

    # Explode adId_vetId to get:
    # adGroupId, targetingClauseId, expression, type, expr_token_count, adId, vetId
    exploded_vetId = get_exploded_vetId(exploded_tc)

    # For debug
    exploded_vetId_asin = exploded_vetId.join(asin_ad, ["adGroupId"], "inner")
    logger.info("Show vetted by examples:")
    exploded_vetId_asin.where(col("vetId") == "1").show(50, False)
    exploded_vetId_asin.where(col("vetId") == "2").show(50, False)
    exploded_vetId_asin.where(col("vetId") == "15").show(50, False)
    exploded_vetId_asin.where(col("vetId") == "17").show(50, False)
    exploded_vetId_asin.where(col("vetId") == "22").show(50, False)

    # For online, all book ad_tc will be set as vetId=9
    # (Exclude book vet data) union all (book tc_ad_vetId=9)

    non_book_vetId = (
        exploded_vetId
            .join(non_book_ad, ["adGroupId", "adId"], "inner")
            .select("adGroupId", "targetingClauseId", "type", "expr_token_count", "adId", "vetId")
    )

    book_vetId = (
        exploded_tc # adGroupId, targetingClauseId, expression, type, adId_vetId, expr_token_count
            .join(book_ad, ["adGroupId"], "inner")
            .select("adGroupId", "targetingClauseId", "type", "expr_token_count", "adId")
            .withColumn("vetId", lit("9"))
    )

    updated_vetId = non_book_vetId.unionByName(book_vetId).distinct()

    # Get vetId distribution
    get_vetId_distribution(updated_vetId, vetId_fieldName)

    # Check the vet in rate
    vetted_tc = (
        updated_vetId
            .select("adGroupId", "targetingClauseId", "type").distinct()
            .groupBy("adGroupId").agg(
                sum(when((col("type") == "exact"), 1).otherwise(0)).alias("vetted_exact_tc_count"),
                sum(when((col("type") == "phrase"), 1).otherwise(0)).alias("vetted_phrase_tc_count"),
                sum(when((col("type") == "broad"), 1).otherwise(0)).alias("vetted_broad_tc_count"),
            )
    )

    vetId_statistics = (
        updated_vetId
            .groupBy("adGroupId").agg( # each <tc_ad> will be vetted in by only one vetId
                countDistinct("targetingClauseId").alias("vetted_tc_count"),
                countDistinct("adId").alias("vetted_ad_count"),
                count("*").alias("vetted_tc_ad_count"),
                sum(when((col("type") == "exact"), 1).otherwise(0)).alias("vetted_exact_tc_ad_count"),
                sum(when((col("type") == "phrase"), 1).otherwise(0)).alias("vetted_phrase_tc_ad_count"),
                sum(when((col("type") == "broad"), 1).otherwise(0)).alias("vetted_broad_tc_ad_count"),
            )
            .join(vetted_tc, ["adGroupId"], "inner")
    )

    # adGroupId, ad_count, tc_count, exact_tc_count, phrase_tc_count, broad_tc_count
    # tc_ad_count, exact_tc_ad_count, phrase_tc_ad_count, broad_tc_ad_count
    # vetted_distinct_tc_count, vetted_distinct_ad_count, vetted_tc_ad_count,
    # vetted_exact_tc_count, vetted_phrase_tc_count, vetted_broad_tc_count
    # vetted_exact_tc_ad_count, vetted_phrase_tc_ad_count, vetted_broad_tc_ad_count
    adGroup_statistics = (
        adGroup_statistics
            .join(vetId_statistics, ["adGroupId"], "inner") # distinct tc and distinct ad
            .withColumn("tc_vet_in_rate", f.round(col("vetted_tc_count")/col("tc_count"), 4))
            .withColumn("ad_vet_in_rate", f.round(col("vetted_ad_count") / col("ad_count"), 4))
            .withColumn("tc_ad_vet_in_rate", f.round(col("vetted_tc_ad_count") / col("tc_ad_count"), 4))
            .withColumn("exact_tc_vet_in_rate", f.round(col("vetted_exact_tc_count") / col("exact_tc_count"), 4))
            .withColumn("phrase_tc_vet_in_rate", f.round(col("vetted_phrase_tc_count") / col("phrase_tc_count"), 4))
            .withColumn("broad_tc_vet_in_rate", f.round(col("vetted_broad_tc_count") / col("broad_tc_count"), 4))
            .withColumn("exact_tc_ad_vet_in_rate", f.round(col("vetted_exact_tc_ad_count") / col("exact_tc_ad_count"), 4))
            .withColumn("phrase_tc_ad_vet_in_rate", f.round(col("vetted_phrase_tc_ad_count") / col("phrase_tc_ad_count"), 4))
            .withColumn("broad_tc_ad_vet_in_rate", f.round(col("vetted_broad_tc_ad_count") / col("broad_tc_ad_count"), 4))
    )

    logger.info("overall_statistics info:")
    overall_statistics = (
        adGroup_statistics.select(
            f.round(sum(col("vetted_tc_count"))/sum(col("tc_count")), 4).alias("tc_vet_in_rate"),
            f.round(sum(col("vetted_ad_count")) / sum(col("ad_count")), 4).alias("ad_vet_in_rate"),
            f.round(sum(col("vetted_tc_ad_count")) / sum(col("tc_ad_count")), 4).alias("tc_ad_vet_in_rate"),
            f.round(sum(col("vetted_exact_tc_count")) / sum(col("exact_tc_count")), 4).alias("exact_tc_vet_in_rate"),
            f.round(sum(col("vetted_phrase_tc_count")) / sum(col("phrase_tc_count")), 4).alias("phrase_tc_vet_in_rate"),
            f.round(sum(col("vetted_broad_tc_count")) / sum(col("broad_tc_count")), 4).alias("broad_tc_vet_in_rate"),
            f.round(sum(col("vetted_exact_tc_ad_count")) / sum(col("exact_tc_ad_count")), 4).alias("exact_tc_ad_vet_in_rate"),
            f.round(sum(col("vetted_phrase_tc_ad_count")) / sum(col("phrase_tc_ad_count")), 4).alias("phrase_tc_ad_vet_in_rate"),
            f.round(sum(col("vetted_broad_tc_ad_count")) / sum(col("broad_tc_ad_count")), 4).alias("broad_tc_ad_vet_in_rate"),
            sum(col("vetted_tc_count")).alias("vetted_tc_count"),
            sum(col("tc_count")).alias("tc_count"),
            sum(col("vetted_ad_count")).alias("vetted_ad_count"),
            sum(col("ad_count")).alias("ad_count"),
            sum(col("vetted_tc_ad_count")).alias("vetted_tc_ad_count"),
            sum(col("tc_ad_count")).alias("tc_ad_count"),
            sum(col("vetted_exact_tc_count")).alias("vetted_exact_tc_count"),
            sum(col("exact_tc_count")).alias("exact_tc_count"),
            sum(col("vetted_phrase_tc_count")).alias("vetted_phrase_tc_count"),
            sum(col("phrase_tc_count")).alias("phrase_tc_count"),
            sum(col("vetted_broad_tc_count")).alias("vetted_broad_tc_count"),
            sum(col("broad_tc_count")).alias("broad_tc_count"),
            sum(col("vetted_exact_tc_ad_count")).alias("vetted_exact_tc_ad_count"),
            sum(col("exact_tc_ad_count")).alias("exact_tc_ad_count"),
            sum(col("vetted_phrase_tc_ad_count")).alias("vetted_phrase_tc_ad_count"),
            sum(col("phrase_tc_ad_count")).alias("phrase_tc_ad_count"),
            sum(col("vetted_broad_tc_ad_count")).alias("vetted_broad_tc_ad_count"),
            sum(col("broad_tc_ad_count")).alias("broad_tc_ad_count"),
        )
    )
    overall_statistics.show(5, False)


if __name__ == "__main__":
    main()
