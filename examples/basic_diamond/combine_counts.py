#!/usr/bin/env python

import glob

total_counts = 0
for counts_file in glob.glob("counts_*.txt"):
    with open(counts_file, mode="r") as file:
        counts = int(file.readline())
        total_counts += counts

with open("total_counts.txt", mode="w") as file:
    file.write(str(total_counts))
