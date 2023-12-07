#!/bin/bash -e

# Setup credentials for download
ada credentials update --account=802575742115 --provider=conduit --role=AdminRole --profile=sp --once

mkdir -p "/tmp/$2"
cd "/tmp/$2"

aws s3 sync "s3://spear-team/ammadupu/rai_ww/djk_input_region/$1" . --profile=sp
mv *.csv rai_ww.tsv

/apollo/env/ClicksDataJackKnife/bin/djk tsv:rai_ww.tsv djf:raiAttributes.djf:marketplaceId,asin --reportout | tee ./run_log.log

/apollo/env/ClicksDataJackKnife/bin/djk djf:raiAttributes.djf head:5

aws s3 sync raiAttributes.djf "s3://spear-team/ammadupu/rai_ww/djk_output/$1" --profile=sp

ada credentials update --account 949168026860 --provider=conduit --role=behavioral-terms-refresh --profile sp-prod --once
aws s3 cp --recursive raiAttributes.djf "s3://behavioral-terms-archive-na/raiAttributes/$2/raiAttributes.djf" --profile=sp-prod                                                          19,1          All