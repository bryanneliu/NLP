#!/bin/bash -e

# Setup credentials for download
ada credentials update --account=802575742115 --provider=conduit --role=AdminRole --profile=sp --once

mkdir -p "/tmp/$2"
cd "/tmp/$2"

aws s3 sync "s3://solr-content-vetting/NA/prod_csv/$1" . --profile=sp
mv *.csv testDataset_na.tsv
# cat *.csv > testDataset_na.tsv

/apollo/env/ClicksDataJackKnife/bin/djk tsv:testDataset_na.tsv djf:test-dataset.djf:marketplaceId,asin --reportout | tee ./run_log.log

/apollo/env/ClicksDataJackKnife/bin/djk djf:test-dataset.djf head:5

aws s3 sync test-dataset.djf "s3://solr-content-vetting/NA/djk_output/$2/test-dataset.djf" --profile=sp

ada credentials update --account 949168026860 --provider=conduit --role=behavioral-terms-refresh --profile sp-prod --once
aws s3 cp --recursive test-dataset.djf "s3://behavioral-terms-archive-na/testDataset/$2/test-dataset.djf" --profile=sp-prod

rm -r "/tmp/$2"
