#!/bin/bash

awk '{print}' ori/wbd.tagset.txt num_unit/tagset.txt age_gender/tagset.txt lemma/lemma.tagset.txt> merged.txt
awk '{ sub("\r", ""); print }' merged.txt > wbd.tagset.txt

rm merged.txt
