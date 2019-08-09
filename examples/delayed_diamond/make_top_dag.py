#!/usr/bin/env python

from pathlib import Path

from htcondor import Submit
import htcondor_dags as dags

dag = dags.DAG()

# This is the "split" step. It stays in the top-level DAG.
# Note that split_words.py no longer takes arguments. It determines the number
# of chunks itself.
split_words = dag.layer(
    name="split_words",
    submit_description=Submit(
        {
            "executable": "split_words.py",
            "transfer_input_files": "words.txt",
            "output": "split_words.out",
            "error": "split_words.err",
        }
    ),
    post=dags.Script(executable="make_analysis_dag.py"),
)

analysis_subdag = split_words.child_subdag(name="analysis", dag_file="analysis.dag")

# Write out the DAG.
# Now that we're going to have two DAG input files in this directory, we need
# to give them unique names.
this_dir = Path(__file__).parent
dag.write(this_dir, dag_file_name="top_level.dag")
print(f"Wrote DAG files to {this_dir}")
