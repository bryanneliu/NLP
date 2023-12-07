#!/bin/bash

paste <(cat sp_sourcing/tokenizer/lemma/unit_test.lemma.txt) <(cat sp_sourcing/tokenizer/lemma/unit_test.lemma.txt | fa_lex --ldb=ldb/tokenizer.bin --tagset=sp_sourcing/tokenizer/wbd.tagset.txt --normalize-input)

paste <(cat sp_sourcing/tokenizer/num_unit/unit_test.num_unit.txt) <(cat sp_sourcing/tokenizer/num_unit/unit_test.num_unit.txt | fa_lex --ldb=ldb/tokenizer.bin --tagset=sp_sourcing/tokenizer/wbd.tagset.txt --normalize-input)

paste <(cat sp_sourcing/tokenizer/num_unit/unit_test.size.txt) <(cat sp_sourcing/tokenizer/num_unit/unit_test.size.txt | fa_lex --ldb=ldb/tokenizer.bin --tagset=sp_sourcing/tokenizer/wbd.tagset.txt --normalize-input)

paste <(cat sp_sourcing/tokenizer/num_unit/unit_test.set.txt) <(cat sp_sourcing/tokenizer/num_unit/unit_test.set.txt | fa_lex --ldb=ldb/tokenizer.bin --tagset=sp_sourcing/tokenizer/wbd.tagset.txt --normalize-input)

paste <(cat sp_sourcing/tokenizer/age_gender/unit_test.kid.age.txt) <(cat sp_sourcing/tokenizer/age_gender/unit_test.kid.age.txt | fa_lex --ldb=ldb/tokenizer.bin --tagset=sp_sourcing/tokenizer/wbd.tagset.txt --normalize-input)
