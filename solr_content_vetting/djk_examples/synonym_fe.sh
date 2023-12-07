#!/bin/bash -e

# Setup credentials for download
ada credentials update --account=802575742115 --provider=conduit --role=AdminRole --profile=sp --once

mkdir -p "/tmp/$2"
cd "/tmp/$2"

aws s3 sync "s3://synonym-expansion-experiments/FE/prod_csv/$1" . --profile=sp
mv *.csv synonyms_fe.tsv
# cat *.csv > synonyms_fe.tsv

/apollo/env/ClicksDataJackKnife/bin/djk tsv:synonyms_fe.tsv djf:synonym-expansion.djf:marketplaceId,asin --reportout | tee ./run_log.log

/apollo/env/ClicksDataJackKnife/bin/djk djf:synonym-expansion.djf head:5

aws s3 sync synonym-expansion.djf "s3://synonym-expansion-experiments/FE/djk_output/$1" --profile=sp

ada credentials update --account 949168026860 --provider=conduit --role=behavioral-terms-refresh --profile sp-prod --once
aws s3 cp --recursive synonym-expansion.djf "s3://behavioral-terms-archive-fe/synonymExpansion/$2/synonym-expansion.djf" --profile=sp-prod                                                  1,1           Top