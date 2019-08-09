#!/usr/bin/env python

import glob

# look for files named count_<something>.txt, which are the output files from
# the previous step
# load each of them, read the number out of it, and add it to our total counts
total_counts = 0
for counts_file in glob.glob("counts_*.txt"):
    with open(counts_file, mode="r") as file:
        counts = int(file.readline())
        total_counts += counts

# write the results to an output file, which HTCondor will detect and transfer
# back to the submit machine
with open("total_counts.txt", mode="w") as file:
    file.write(str(total_counts))
