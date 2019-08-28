#!/usr/bin/env python

import sys

num_chunks = int(sys.argv[1])

# read all of the words out of words.txt
# since each word is on its own line, we can just use readlines()
with open("words.txt", mode="r") as file:
    words = file.readlines()

# calculate how many words should go in each chunk
# if it wasn't evenly-divisible by num_splits, we would need to do some extra
# work to catch stragglers...
words_per_chunk = int(len(words) / num_chunks)

# slice the words list into chunks, writing each one to a file
# HTCondor will notice the new files and transfer them back to the submit host
for chunk in range(num_chunks):
    print("Writing chunk {}".format(chunk))

    with open("words_{}.txt".format(chunk), mode="w") as file:
        file.writelines(words[chunk * words_per_chunk : (chunk + 1) * words_per_chunk])
