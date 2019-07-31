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


def test_empty_dag_writes_empty_dagfile(dag_dir):
    dag = dags.DAG()
    dag.write(dag_dir)

    # if there are any lines in the file, they must be comments
    assert all(
        line.startswith("#")
        for line in (dag_dir / dags.DAG_FILE_NAME).read_text().splitlines()
    )


def test_jobstate_log_as_str(dag_dir):
    logfile = "i_am_the_jobstate.log"

    dag = dags.DAG(jobstate_log=logfile)
    dag.write(dag_dir)

    assert f"\nJOBSTATE_LOG {logfile}\n" in (dag_dir / dags.DAG_FILE_NAME).read_text()


def test_jobstate_log_as_path(dag_dir):
    logfile = Path("i_am_the_jobstate.log").absolute()

    dag = dags.DAG(jobstate_log=logfile)
    dag.write(dag_dir)

    assert (
        f"\nJOBSTATE_LOG {logfile.as_posix()}\n"
        in (dag_dir / dags.DAG_FILE_NAME).read_text()
    )


def test_config_file_gets_written_if_config_given(dag_dir):
    dag = dags.DAG(config={"DAGMAN_MAX_JOBS_IDLE": 10})
    dag.write(dag_dir)

    assert (dag_dir / dags.CONFIG_FILE_NAME).exists()


def test_config_command_gets_written_if_config_given(dag_dir):
    dag = dags.DAG(config={"DAGMAN_MAX_JOBS_IDLE": 10})
    dag.write(dag_dir)

    assert (
        f"\nCONFIG {dags.CONFIG_FILE_NAME}\n"
        in (dag_dir / dags.DAG_FILE_NAME).read_text()
    )


def test_config_file_has_right_contents(dag_dir):
    dag = dags.DAG(config={"DAGMAN_MAX_JOBS_IDLE": 10})
    dag.write(dag_dir)

    assert "DAGMAN_MAX_JOBS_IDLE = 10" in (dag_dir / dags.CONFIG_FILE_NAME).read_text()
