#!/bin/bash

paste <(cat sp_sourcing/tokenizer/unit_test/wrong_cases.txt) <(cat sp_sourcing/tokenizer/unit_test/wrong_cases.txt | fa_lex --ldb=ldb/tokenizer.bin --tagset=sp_sourcing/tokenizer/wbd.tagset.txt --normalize-input)

