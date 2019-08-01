#!/usr/bin/env python

import sys

num_splits = int(sys.argv[1])


with open("words.txt", mode="r") as file:
    words = file.readlines()

print(len(words))

words_per_chunk = int(len(words) / num_splits)

for chunk in range(num_splits):
    print("Writing chunk {}".format(chunk))

    with open("words_{}.txt".format(chunk), mode="w") as file:
        file.writelines(words[chunk * words_per_chunk : (chunk + 1) * words_per_chunk])
