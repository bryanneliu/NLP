import pyspark.sql.types as t
from pyspark.sql.functions import udf
import json
import argparse

'''
indexable schema:
https://code.amazon.com/packages/ProductAdsSearchConfig/blobs/mainline/--/app_confs/sp/solr/sp/conf/schema.xml
'''
INDEXABLE_AD_ELEMENT_SCHEMA = t.StructType([
    t.StructField('adId', t.StringType()),
    t.StructField('adVersion', t.StringType()),
    t.StructField('title', t.StringType()),
    t.StructField('asin', t.StringType()),
    t.StructField('brand', t.StringType()),
    t.StructField('glProductGroupType', t.StringType()),
    t.StructField('browseLadder', t.ArrayType(t.StringType())),
    t.StructField('decorationId', t.StringType()),
    t.StructField('globalUniqueItemKey', t.StringType()),
    t.StructField('eventId', t.StringType()),
    t.StructField('expression', t.StringType()),
    t.StructField('pk', t.StringType()),
])

INDEXABLE_CLAUSE_ELEMENT_SCHEMA = t.StructType([
    t.StructField('adId_vetId', t.StringType()),
    t.StructField('adId_vetScore', t.StringType()),
    t.StructField('targetingClauseId', t.StringType()),
    t.StructField('expression', t.StringType()),
    t.StructField('type', t.StringType()),
    t.StructField('bid', t.StringType())
])

INDEXABLE_PROJECTED_SCHEMA = t.StructType([
    t.StructField('marketplaceId', t.StringType()),
    t.StructField('advertiserId', t.StringType()),
    t.StructField('businessProgramId', t.StringType()),
    t.StructField('campaignId', t.StringType()),
    t.StructField('adGroupId', t.StringType()),
    t.StructField('adGroupIdPart', t.StringType()),
    t.StructField('portfolioId', t.StringType()),
    t.StructField('adVersion', t.StringType()),
    t.StructField('negativeKeywordListId', t.ArrayType(t.StringType())),
    t.StructField('isForwardSearchable', t.StringType()),
    t.StructField('hasTargetingExpression', t.StringType()),
    t.StructField('child', t.ArrayType(INDEXABLE_CLAUSE_ELEMENT_SCHEMA)),
    t.StructField('ad', t.ArrayType(INDEXABLE_AD_ELEMENT_SCHEMA))
])


'''
# SolrConfig.xml
# https://code.amazon.com/packages/ProductAdsSearchConfig/blobs/heads/mainline/--/app_confs/sp/solr/sp/conf/solrconfig.xml
# vetId to fieldName mapping
'''
vetId_fieldName_schema = t.StructType([
    t.StructField("vetId", t.StringType(), nullable=False),
    t.StructField("fieldName", t.StringType(), nullable=False)
])

vetId_fieldName_mapping = ([
        ("0", "AT"),
        ("1", "vetPhase1:title"),
        ("9", "gl_book"),
        ("10", "close-match"),
        ("11", "loose-match"),
        ("8", "vetPhase2:authors,targetAudienceKeywords"),
        ("2", "vetPhase3:phrasedocTerms"),
        ("3", "vetPhase4:termdocTerms"),
        ("4", "vetPhase5:generic_keywords"),
        ("5", "vetPhase6:brand,binding,subjects,platforms,genr"),
        ("7", "vetPhase7:browseNodeNames"),
        ("13", "vetPhase8:organicdocTerms"),
        ("14", "vetPhase9:semanticDLdocTerm"),
        ("6", "vetPhase10:bulletPoint"),
        ("15", "vetPhase11:exp3_bow"),
        ("16", "vetPhase12:xdf_softlines"),
        ("17", "vetPhase13:adCoherance"),
        ("18", "vetPhase14:bbq10"),
        ("19", "vetPhase15:bookBrowseNodeNames"),
        ("20", "vetPhase16:krsKeywordRecommendationsUniqueUnigrams"),
        ("21", "vetPhase17:raiAttributes"),
        ("22", "vetPhase18:synonyms"),
        ("999", "vetPhase999:testDataset")
       ]
)


'''
print(parseVetId("""["200144034138098,2","200144034138198,2"]"""))
print(parseVetId("200144034138098,2"))
print(parseVetId(None))
'''
def parseVetId(s):
    if not s:
        return []
    if not s.startswith("["):
        return [s]
    return json.loads(s)

parseVetId_udf = udf(parseVetId, t.ArrayType(t.StringType()))


glProductGroupType_book = ["gl_book", "gl_ebooks", "gl_digital_book_service", "gl_digital_ebook_purchase", "gl_audible"]


def process_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--marketplace_id",
        type=int,
        default=1,
        help="Specify marketplaceid",
        required=True
    )

    parser.add_argument(
        "--s3_indexable",
        type=str,
        default="s3://adsdelivery-index-na-master/Master2/indexables/coldstart/sp_1/indexable-json/adVersion=1693760188203/",
        help="Specify S3 indexable Json path",
        required=True
    )

    parser.add_argument(
        "--s3_janus",
        type=str,
        default="s3://adsdelivery-index-na-master/Master2/indexables/coldstart/sp*/janus-json/adVersion=1695847083841/",
        help="Specify S3 Janus json path",
        required=True
    )

    parser.add_argument(
        "--task",
        type=str,
        default="content_vetting",
        help="Supported tasks: content_vetting, index_distribution, asin_popularity",
        required=True
    )

    args, _ = parser.parse_known_args()

    return args


# Prod Indexable Path
# https://conduit.security.a2z.com/accounts/aws/949168026860/attributes
'''
na has 16 shards: sp, sp_1, ..., sp_15
s3_indexable_na = "s3://adsdelivery-index-na-master/Master2/indexables/coldstart/sp*/indexable-json/adVersion=1693760188203/"
s3_janus_na = "s3://adsdelivery-index-na-master/Master2/indexables/coldstart/sp*/janus-json/adVersion=1695847083841/"

eu has 8 shards: sp, sp_1, ..., sp_7
s3_indexable_eu = "s3://adsdelivery-index-eu-master/Master2/indexables/coldstart/sp*/indexable-json/adVersion=1694036472032/"
s3_janus_eu = "s3://adsdelivery-index-eu-master/Master2/indexables/coldstart/sp*/janus-json/adVersion=1696103418175/"

fe has 4 shards: sp, sp_1, ..., sp_3
s3_indexable_fe = "s3://adsdelivery-index-fe-master/Master2/indexables/coldstart/sp*/indexable-json/adVersion=1694567296988/"
s3_janus_fe = "s3://adsdelivery-index-fe-master/Master2/indexables/coldstart/sp*/janus-json/adVersion=1696188442482/"
'''

'''
root
 |-- ad: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- adId: string (nullable = true)
 |    |    |-- adVersion: string (nullable = true)
 |    |    |-- asin: string (nullable = true)
 |    |    |-- authors: string (nullable = true)
 |    |    |-- averageRating: string (nullable = true)
 |    |    |-- binding: string (nullable = true)
 |    |    |-- brand: string (nullable = true)
 |    |    |-- browseLadder: string (nullable = true)
 |    |    |-- buyabilityPrice: string (nullable = true)
 |    |    |-- color: string (nullable = true)
 |    |    |-- decorationId: string (nullable = true)
 |    |    |-- department: string (nullable = true)
 |    |    |-- eventId: string (nullable = true)
 |    |    |-- generic_keywords: string (nullable = true)
 |    |    |-- genre: string (nullable = true)
 |    |    |-- glProductGroupType: string (nullable = true)
 |    |    |-- inactiveReason: string (nullable = true)
 |    |    |-- indexDeleteDocument: string (nullable = true)
 |    |    |-- manufacturer: string (nullable = true)
 |    |    |-- merchantSku: string (nullable = true)
 |    |    |-- model: string (nullable = true)
 |    |    |-- offerListingType: string (nullable = true)
 |    |    |-- platforms: string (nullable = true)
 |    |    |-- reviewCount: string (nullable = true)
 |    |    |-- size: string (nullable = true)
 |    |    |-- specialType: string (nullable = true)
 |    |    |-- targetAudienceKeywords: string (nullable = true)
 |    |    |-- title: string (nullable = true)
 |    |    |-- variationParentAsin: string (nullable = true)
 |-- adGroupId: string (nullable = true)
 |-- adGroupNegativeKeywordListId: string (nullable = true)
 |-- adVersion: string (nullable = true)
 |-- advertiserId: string (nullable = true)
 |-- bid: string (nullable = true)
 |-- businessProgramId: string (nullable = true)
 |-- campaignId: string (nullable = true)
 |-- clause: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- bid: string (nullable = true)
 |    |    |-- expression: string (nullable = true)
 |    |    |-- subId: string (nullable = true)
 |    |    |-- targetingClauseId: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |-- hasTargetingExpression: string (nullable = true)
 |-- indexDeleteDocument: string (nullable = true)
 |-- isForwardSearchable: string (nullable = true)
 |-- isTest: string (nullable = true)
 |-- marketplaceId: string (nullable = true)
 |-- negativeKeywordListId: string (nullable = true)
 |-- portfolioId: string (nullable = true)
 |-- targetingClauseId: string (nullable = true)
 |-- venue: string (nullable = true)
'''