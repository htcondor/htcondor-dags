#!/usr/bin/env python

import sys

# get the input file number from the arguments list
word_set = sys.argv[1]
word_file = "words_{}.txt".format(word_set)

count = 0
with open(word_file, mode="r") as file:
    for word in file:
        if len(word) < 5:
            count += 1

result_file = "counts_{}.txt".format(word_set)
with open(result_file, mode="w") as file:
    file.write(str(count))
