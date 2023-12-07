#!/usr/bin/env python3

import glob
import re

with open("wbd.lex.utf8", "w") as rules:
    for file in glob.glob("wbd.*.vocab.utf8"):
        _, tag_raw, _, _ = file.split(".")
        tag = tag_raw.upper()
        patterns = []

        with open(file) as f:
            for pattern in f.readlines():
                pattern = pattern.strip()
                if not pattern:
                    continue
                # 50 year old woman's shoes -> 50[ ]year[ ]old[ ]woman's[ ]shoes
                # TODO: handle regex special characters here as needed
                spaced_pattern = "[ ]".join(pattern.split(" "))
                patterns.append(spaced_pattern)

        rules.write(
            "< " + "\n  | ".join([f"({p})" for p in patterns]) + f"\n> --> {tag}\n"
        )
