#!/usr/bin/env python

from pathlib import Path

from htcondor import Submit
import htcondor_dags as dags

dag = dags.DAG()

NUM_SPLITS = 10

split_data = dag.layer(
    name="split_data",
    submit_description=Submit(
        {
            "executable": "split_data.py",
            "arguments": str(NUM_SPLITS),
            "transfer_input_files": "words.txt",
        }
    ),
)

count_words = split_data.child(
    name="count_words",
    submit_description=Submit(
        {
            "executable": "count_words.py",
            "arguments": "$(word_set)",
            "transfer_input_files": "words_$(word_set).txt",
        }
    ),
    vars=[{"word_set": str(n)} for n in range(NUM_SPLITS)],
)

combine_analysis = count_words.child(
    name="combine_counts",
    submit_description=Submit(
        {
            "executable": "combine_counts.py",
            "transfer_input_files": ", ".join(
                f"counts_{n}.txt" for n in range(NUM_SPLITS)
            ),
        }
    ),
)

this_dir = Path(__file__).parent
dag_dir = this_dir / "basic_diamond_dag"
dag.write(dag_dir)

print(f"Wrote DAG files to {dag_dir}")
