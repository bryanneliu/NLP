# vetId definitions
# https://code.amazon.com/packages/ProductAdsSearchCustom/blobs/454ac68c633184b91962b42fc6d19c4fc2cc9fab/--/src/com/amazon/productads/solr/update/processor/MetricsUpdateProcessorFactory.java#L496
public void inform(SolrCore core) {
    AmazonConfig amazonConfig = ConfigUtils.getAmazonConfig(core.getSolrConfig());

    vetIds.add(0); // add vetId=0 for AT, which is not in solr config
    vetIds.add(9); // add vetId=9 for vet overrides, also not in solr config
    vetIds.add(10); // add vetId=10 for ATv2 close-match, also not in solr config
    vetIds.add(11); // add vetId=11 for ATv2 loose-match, also not in solr config
    vetIds.addAll(amazonConfig.getVetIds().values());
    log.info("vetIds={}", vetIds);

    countryMap.putAll(amazonConfig.getCountryMap());
    log.info("CountryMap={}", countryMap);
}

# SolrConfig.xml
# https://code.amazon.com/packages/ProductAdsSearchConfig/blobs/heads/mainline/--/app_confs/sp/solr/sp/conf/solrconfig.xml
<vetting>
    <!--<entry id="0">autotargeted</entry>-->
    <entry id="1">vetPhase1:title</entry>
    <entry id="8">vetPhase2:authors,targetAudienceKeywords</entry>
    <entry id="2">vetPhase3:phrasedocTerms</entry>
    <entry id="3">vetPhase4:termdocTerms</entry>
    <entry id="4">vetPhase5:generic_keywords</entry>
    <entry id="5">vetPhase6:brand,binding,subjects,platforms,genre</entry>
    <entry id="7">vetPhase7:browseNodeNames</entry>
    <entry id="13">vetPhase8:organicdocTerms</entry>
    <entry id="14">vetPhase9:semanticDLdocTerms</entry>
    <entry id="6">vetPhase10:bulletPoint</entry>
    <entry id="15">vetPhase11:exp3_bow</entry>
    <entry id="16">vetPhase12:xdf_softlines</entry>
    <entry id="17">vetPhase13:adCoherance</entry>
    <entry id="18">vetPhase14:bbq10</entry>
    <entry id="19">vetPhase15:bookBrowseNodeNames</entry>
    <entry id="20">vetPhase16:krsKeywordRecommendationsUniqueUnigrams</entry>
    <entry id="21">vetPhase17:raiAttributes</entry>
    <entry id="22">vetPhase18:synonyms</entry>
    <entry id="999">vetPhase999:testDataset</entry>
    <!-- <entry id="7">vetPhase5:synonyms</entry> -->
    <!-- <entry id="8">broadening</entry> enables broadening -->
    <!-- <entry id="9">vet override</entry>  -->
    <!-- <entry id="10">close-match</entry>  -->
    <!-- <entry id="11">loose-match</entry>  -->
    <!-- <entry id="12">whitelist</entry>  -->
    <!-- <entry id="-1">synonyms:title</entry> enables synonyms over the listed fields -->
</vetting>

# Wiki about static index & indexable
# https://w.amazon.com/bin/view/ProductAds/SearchIndex/ServiceAlarmsAndSOPs/#Static_Index
# s3://adsdelivery-index-na-master/Master2/indexables/coldstart/sp_1/
# s3://adsdelivery-index-na-master/Master2/indexables/incremental/sp_1/

# Example code to parse janus-json and indexable-json
# https://code.amazon.com/packages/SpearAnalysisScripts/blobs/mainline/--/src/analysis/solr/solr_index_analysis/EU_latency_analysis.ipynb
# https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1#/editor/job/Solr%20Index%20NA/script
# https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1#/editor/job/Solr%20Index%20EU/script
# https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1#/editor/job/Solr%20Index%20NA/script
# https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1#/editor/job/Solr%20Index%20FE/script

# Axiom
# https://w.amazon.com/bin/view/ProductAds/Development/CampaignPipelinePlatform/#Entity_Relationship_Diagram
# Advertisers create an ad group, that has list of adIds and tcIds
* Advertiser → campaign → adgroup → (ad, kw)
* AT and MT are in different adGroups
* Negative keywords are on campaign level or adgroup level
    * (janus.negativekeywordlistid or janus.adgroupnegativekeywordlistid) = janus_nkw.negativekeywordlistid
* ATv1 vs ATv2 vs MT
    * hasTargetingExpression:true → ATv2 with clauses close and loose
        * hasTargetingExpression is null && isForwardSearchable:true → ATv1 (NOT exist in Janus)
    * isForwardSearchable is null → MT with clauses exact, phrase, broad

# AdGroup-based Index
# https://quip-amazon.com/pm0uAIwnLp4Z/Ad-Grouped-Index-Launch-Announcement

# Shard Structure
# NA has 16 shards and EU has 8 shards, sharded by advertiserId. sp is shard 0.
# https://w.amazon.com/bin/view/ProductAds/Development/Sourcing/SolrTuningParameters/#HSolrshards-effectsofincreasingshardcount

# Content Vetting Prod Metrics
# Indexer metrics & content vetting metrics & code
# https://w.amazon.com/bin/view/ProductAds/SearchIndex/Dashboard/SP/Prod/Indexers/NA/#HIndexerMetrics
# https://tiny.amazon.com/16f3cpach/iGraph
# https://code.amazon.com/packages/ProductAdsSearchCustom/blobs/454ac68c633184b91962b42fc6d19c4fc2cc9fab/--/src/com/amazon/productads/solr/update/processor/MetricsUpdateProcessorFactory.java#L496


# Content vetting special logic
# Adcoherence vetId 17 has dropped 50% since launch in NA in 8/2022
# https://tiny.amazon.com/d8diw1jx/iGraph

# No vetting
# https://code.amazon.com/packages/ProductAdsSearchConfig/blobs/ad7bc66435cd6ec2f0ea8098cc55ff16dd9509b7/--/app_confs/sp/djk-expression-default-values.properties#L28
# NO_VETTING_GLS=glProductGroupType:=gl_book|glProductGroupType:=gl_ebooks|glProductGroupType:=gl_digital_book_service|glProductGroupType:=gl_digital_ebook_purchase|glProductGroupType:=gl_audible
# For book, remove the book adId and remove it from vettedBy
# Construct a table like: [adId, tcId, vetid, glGroup]
# vetid=9 is 99.99% books, there are 1 exception but don't think its worth worrying, which is that we vet override in Janus as well

# Summary:
# If the tc is de-duped, the tc expression field is empty
# For vet_id=9, gl_book, all keywords are in expression, book team manages irrelevance and end to end flow
# adId is unique, asin can be mapped to multiple adId. ASIN with nkw and without nkw map to different adIds.
# adId_vetId is an array which contains values like "adIdX,17" to show that tcId and adIdX was vetted in by 17, if there's no entry for adId, then that tcId was not vetted in at all for that adId
# At the indexable level its a single field negativeKeywordListId, at this point we don't know if its campaign of adgroup. There is a way to find out, listId has a bitmask to show if its campaign of adgorup, but easier would be to join with janus data instead

'''
Vaibhav Goel
QQ: What is the use case where advertiser will have same ASIN in multiple Ads?
If advertiser wants different bid for different keywords, they will add different targeting clause.
Ref: I am watching Brownbag video by Asim on Solr and it shows that in NA, we have 140MM Ads and 90MM ASINs.

Asim Mahmood
1. there are 2 kinds of advertiser: sellers own own SKU and vendors who own the asin
2. advertiser can create an auto and manual campaign for same asin
3. larger advertiser tend to create multiple campaigns with staggered start and end date with different strategies, e.g. PD
4. Since there’s no cost to advertisers to create campaigns, agencies also tend to create multiple campaigns and optimize
'''

# Disable the content vetting
'''
With adGroup based index, all targeting clauses are in the index, use the field vettedBy to denote set of tuple of adId-vetId that this keyword was vetted against.
Handler exact_p1_novet which opens up the content vetting increased numFound by 2% (tested several years ago and tested recently):
exmatch_p1_novet is not exactly the same as exmatch_p1 with vetting.enabled=false parameter set.
The novet doesn't return atv1 ads, but exmatch_p1 does.
'''

# https://quip-amazon.com/kVGvADOkVRsx/Solr-Content-Vetting-Gaps-and-Improvements-plan

# Posting-list
'''
How do we build posting-list? and where do we save it?
we don't build posting list, we pass the data to lucene at index time and it builds it internally, its not exposed at the API level
we can reverse-engineer it via some code for debug purpose but it'll be slow.

We have adQuery and tcQuery logic.
How do we do adQuery (query, ASIN) match? We built term inverted-index (posting-list) for ASIN expression_field.
How do we do tcQuery (query, keyword) match?
We do use the same API but instead creating invested index of individual terms in keyword,
always use single term for all keywords so becomes like a map look up.
e.g. input: "iphone case" phase match -> iphone_case posting-list
'''

# AdGroup update and ad joins with tc:
ads: https://code.amazon.com/packages/ProductAdsSearchCustom/blobs/mainline/--/src/com/am[…]tads/solr/update/processor/AdGroupUpdateProcessorFactory.java
keywords: https://code.amazon.com/packages/ProductAdsSearchCustom/blobs/mainline/--/src/com/am[…]r/update/processor/TargetingClauseUpdateProcessorFactory.java
https://code.amazon.com/packages/ProductAdsSearchCustom/blobs/mainline/--/tst/com/am[…]/solr/update/processor/AdGroupUpdateProcessorFactoryTest.java - think it tests both above
https://code.amazon.com/packages/ProductAdsSearchCustom/blobs/mainline/--/src/com/amazon/productads/solr/search/parentchild/ParentChildJoinQueryImpl.java

# Parse vetted-by and vet-id
https://code.amazon.com/packages/ProductAdsSearchConfig/blobs/04c52c4dfcf6cb3e0c01f24f6b7dc3fc9ed698ec/--/app_confs/sp/solr/sp/conf/schema.xml#L162,L163
https://code.amazon.com/packages/ProductAdsSearchCustom/blobs/mainline/--/src/com/amazon/productads/solr/update/processor/TargetingClauseUpdateProcessorFactory.java#L181
# tcDocument.addField(FieldConstants.VETTED_BY_FIELD, ExmatchParserUtils.encodeOffsetAndVet(offset, vetId));
# tcDocument.addField(FieldConstants.VET_ID, vetId);
# prod content vetting metrics are for tc level: the tc is vetted in at least 1 ad with the vetId

# indexable schema:
https://code.amazon.com/packages/ProductAdsSearchConfig/blobs/mainline/--/app_confs/sp/solr/sp/conf/schema.xml
Define the dynamic fields to index: expression_field and keyword_field indexable=true
https://code.amazon.com/packages/DJKClicksCommons/blobs/3aa9b5cd81d89ca9b0a666278d02cb321515975e/--/src/com/amazon/clicks/djk/vetting/TextNormalizerPipe.java#L50

# SolrConfig.xml
# https://code.amazon.com/packages/ProductAdsSearchConfig/blobs/heads/mainline/--/app_confs/sp/solr/sp/conf/solrconfig.xml
# vetId to fieldName mapping