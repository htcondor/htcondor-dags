# Copyright 2019 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path

import htcondor_dags as dags

from .conftest import dagfile_lines


def test_empty_dag_writes_empty_dagfile(dag_dir):
    dag = dags.DAG()
    dags.write_dag(dag, dag_dir)

    # if there are any lines in the file, they must be comments
    assert all(line.startswith("#") for line in dagfile_lines(dag_dir))


def test_jobstate_log_as_str(dag_dir):
    logfile = "i_am_the_jobstate.log"

    dag = dags.DAG(jobstate_log=logfile)
    dags.write_dag(dag, dag_dir)

    assert f"JOBSTATE_LOG {logfile}" in dagfile_lines(dag_dir)


def test_jobstate_log_as_path(dag_dir):
    logfile = Path("i_am_the_jobstate.log").absolute()

    dag = dags.DAG(jobstate_log=logfile)
    dags.write_dag(dag, dag_dir)

    assert f"JOBSTATE_LOG {logfile.as_posix()}" in dagfile_lines(dag_dir)


def test_config_file_gets_written_if_config_given(dag_dir):
    dag = dags.DAG(dagman_config={"DAGMAN_MAX_JOBS_IDLE": 10})
    dags.write_dag(dag, dag_dir)

    assert (dag_dir / dags.CONFIG_FILE_NAME).exists()


def test_config_command_gets_written_if_config_given(dag_dir):
    dag = dags.DAG(dagman_config={"DAGMAN_MAX_JOBS_IDLE": 10})
    dags.write_dag(dag, dag_dir)

    assert (
        f"\nCONFIG {dags.CONFIG_FILE_NAME}\n"
        in (dag_dir / dags.DEFAULT_DAG_FILE_NAME).read_text()
    )


def test_config_file_has_right_contents(dag_dir):
    dag = dags.DAG(dagman_config={"DAGMAN_MAX_JOBS_IDLE": 10})
    dags.write_dag(dag, dag_dir)

    assert (
        "DAGMAN_MAX_JOBS_IDLE = 10"
        in (dag_dir / dags.CONFIG_FILE_NAME).read_text().splitlines()
    )


def test_dagman_job_attributes_with_one_attr(dag_dir):
    dag = dags.DAG(dagman_job_attributes={"foo": "bar"})
    dags.write_dag(dag, dag_dir)

    assert "SET_JOB_ATTR foo = bar" in dagfile_lines(dag_dir)


def test_dagman_job_attributes_with_two_attrs(dag_dir):
    dag = dags.DAG(dagman_job_attributes={"foo": "bar", "wizard": 17})
    dags.write_dag(dag, dag_dir)

    contents = dagfile_lines(dag_dir)
    assert all(
        ("SET_JOB_ATTR foo = bar" in contents, "SET_JOB_ATTR wizard = 17" in contents)
    )


def test_max_jobs_per_category_with_one_category(dag_dir):
    dag = dags.DAG(max_jobs_by_category={"foo": 5})
    dags.write_dag(dag, dag_dir)

    assert "CATEGORY foo 5" in dagfile_lines(dag_dir)


def test_max_jobs_per_category_with_two_categories(dag_dir):
    dag = dags.DAG(max_jobs_by_category={"foo": 5, "bar": 10})
    dags.write_dag(dag, dag_dir)

    contents = dagfile_lines(dag_dir)
    assert all(("CATEGORY foo 5" in contents, "CATEGORY bar 10" in contents))


def test_dot_config_default(dag_dir):
    dag = dags.DAG(dot_config=dags.DotConfig("dag.dot"))
    dags.write_dag(dag, dag_dir)

    assert "DOT dag.dot DONT-UPDATE OVERWRITE" in dagfile_lines(dag_dir)


def test_dot_config_not_default(dag_dir):
    dag = dags.DAG(
        dot_config=dags.DotConfig(
            "dag.dot", update=True, overwrite=False, include_file="include-me.dot"
        )
    )
    dags.write_dag(dag, dag_dir)

    assert "DOT dag.dot UPDATE DONT-OVERWRITE INCLUDE include-me.dot" in dagfile_lines(
        dag_dir
    )


def test_node_status_file_default(dag_dir):
    dag = dags.DAG(node_status_file=dags.NodeStatusFile("node_status_file"))
    dags.write_dag(dag, dag_dir)

    assert "NODE_STATUS_FILE node_status_file"


def test_node_status_file_not_default(dag_dir):
    dag = dags.DAG(
        node_status_file=dags.NodeStatusFile(
            "node_status_file", update_time=60, always_update=True
        )
    )
    dags.write_dag(dag, dag_dir)

    assert "NODE_STATUS_FILE node_status_file 60 ALWAYS-UPDATE"
