from pyspark.sql.functions import lit, col, count

def prepare_region_data(s3_input, s3_output):
    region = spark.read.option("delimiter", "\t").csv(s3_input)
    region = (
        region
            .withColumnRenamed("_c0", "asin")
            .withColumnRenamed("_c1", "marketplaceId")
            .withColumnRenamed("_c2", "synonyms")
    )
    region = region.withColumn("synonyms_isTextNormalized", lit("true"))

    region = region.coalesce(1)
    (
        region.write
            .mode("overwrite")
            .option("delimiter", "\t")
            .option("header", True)
            .csv(s3_output)
    )

    region.printSchema()
    region.show(10, False)

    markets = region.groupBy("marketplaceId").agg(count("asin").alias("asin_count"))
    markets.show(20, False)

    test = spark.read.csv(s3_output)
    test.show(10, False)

####################
# Prepare FE dataset
####################
s3_input = "s3://synonym-expansion-experiments/FE/*/solr_index_2023-08-08/dataset_csv/threshold_10/"
s3_output = "s3://synonym-expansion-experiments/FE/prod_csv/threshold_10/"
prepare_region_data(s3_input, s3_output)

s3_input = "s3://synonym-expansion-experiments/FE/*/solr_index_2023-08-08/dataset_csv/threshold_15/"
s3_output = "s3://synonym-expansion-experiments/FE/prod_csv/threshold_15/"
prepare_region_data(s3_input, s3_output)

s3_input = "s3://synonym-expansion-experiments/FE/*/solr_index_2023-08-08/dataset_csv/threshold_20/"
s3_output = "s3://synonym-expansion-experiments/FE/prod_csv/threshold_20/"
prepare_region_data(s3_input, s3_output)

####################
# Prepare EU dataset
####################
s3_input = "s3://synonym-expansion-experiments/EU/*/solr_index_2023-08-08/dataset_csv/threshold_5/"
s3_output = "s3://synonym-expansion-experiments/EU/prod_csv/threshold_5/"
prepare_region_data(s3_input, s3_output)

s3_input = "s3://synonym-expansion-experiments/EU/*/solr_index_2023-08-08/dataset_csv/threshold_10/"
s3_output = "s3://synonym-expansion-experiments/EU/prod_csv/threshold_10/"
prepare_region_data(s3_input, s3_output)

s3_input = "s3://synonym-expansion-experiments/EU/*/solr_index_2023-08-08/dataset_csv/threshold_15/"
s3_output = "s3://synonym-expansion-experiments/EU/prod_csv/threshold_15/"
prepare_region_data(s3_input, s3_output)

s3_input = "s3://synonym-expansion-experiments/EU/*/solr_index_2023-08-08/dataset_csv/threshold_20/"
s3_output = "s3://synonym-expansion-experiments/EU/prod_csv/threshold_20/"
prepare_region_data(s3_input, s3_output)