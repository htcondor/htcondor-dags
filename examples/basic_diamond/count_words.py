#!/usr/bin/env python

import sys

# get the input file number from the arguments list
word_set = sys.argv[1]
word_file = "words_{}.txt".format(word_set)

# iterate over the lines in the file, each of which contains one word
# if the length is less than 5, increment the counter
count = 0
with open(word_file, mode="r") as file:
    for word in file:
        if len(word) < 5:
            count += 1

# write the results to an output file, which HTCondor will detect and transfer
# back to the submit machine
result_file = "counts_{}.txt".format(word_set)
with open(result_file, mode="w") as file:
    file.write(str(count))
