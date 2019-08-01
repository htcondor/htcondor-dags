#!/usr/bin/env python

from pathlib import Path

from htcondor import Submit
import htcondor_dags as dags

dag = dags.DAG()

NUM_SPLITS = 10

split_words = dag.layer(
    name="split_words",
    submit_description=Submit(
        {
            "executable": "split_words.py",
            "arguments": str(NUM_SPLITS),
            "transfer_input_files": "words.txt",
            "output": "split_words.out",
            "error": "split_words.err",
        }
    ),
)

count_words = split_words.child(
    name="count_words",
    submit_description=Submit(
        {
            "executable": "count_words.py",
            "arguments": "$(word_set)",
            "transfer_input_files": "words_$(word_set).txt",
            "output": "count_words_$(word_set).out",
            "error": "count_words_$(word_set).err",
        }
    ),
    vars=[{"word_set": str(n)} for n in range(NUM_SPLITS)],
)

combine_counts = count_words.child(
    name="combine_counts",
    submit_description=Submit(
        {
            "executable": "combine_counts.py",
            "transfer_input_files": ", ".join(
                f"counts_{n}.txt" for n in range(NUM_SPLITS)
            ),
            "output": "combine_counts.out",
            "error": "combine_counts.err",
        }
    ),
)

this_dir = Path(__file__).parent
dag.write(this_dir)

print(f"Wrote DAG files to {this_dir}")
