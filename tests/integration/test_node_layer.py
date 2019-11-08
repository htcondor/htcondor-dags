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

import pytest

from pathlib import Path

import htcondor_dags as dags
from .conftest import dagfile_lines, dagfile_text


def test_layer_name_appears(dag_dir, dag):
    dag.layer(name="foobar")

    dags.write_dag(dag, dag_dir)

    assert "foobar" in dagfile_text(dag_dir)


def test_job_line_for_no_vars(dag_dir, dag):
    dag.layer(name="foobar")

    dags.write_dag(dag, dag_dir)

    assert "JOB foobar foobar.sub" in dagfile_lines(dag_dir)


def test_job_line_for_one_vars(dag_dir, dag):
    dag.layer(name="foobar", vars=[{"bing": "bang"}])

    dags.write_dag(dag, dag_dir)

    assert "JOB foobar foobar.sub" in dagfile_lines(dag_dir)


def test_job_lines_for_two_vars(dag_dir, dag):
    dag.layer(name="foobar", vars=[{"bing": "bang"}, {"bing": "bong"}])

    dags.write_dag(dag, dag_dir)

    lines = dagfile_lines(dag_dir)
    assert f"JOB foobar{dags.SEPARATOR}0 foobar.sub" in lines
    assert f'VARS foobar{dags.SEPARATOR}0 bing="bang"' in lines
    assert f"JOB foobar{dags.SEPARATOR}1 foobar.sub" in lines
    assert f'VARS foobar{dags.SEPARATOR}1 bing="bong"' in lines


def test_node_inline_meta(dag_dir, dag):
    dag.layer(name="foobar", dir="dir", noop=True, done=True)

    dags.write_dag(dag, dag_dir)

    assert "JOB foobar foobar.sub DIR dir NOOP DONE" in dagfile_lines(dag_dir)


def test_layer_retry(dag_dir, dag):
    dag.layer(name="foobar", retries=5)

    dags.write_dag(dag, dag_dir)

    assert "RETRY foobar 5" in dagfile_lines(dag_dir)


def test_layer_retry_with_unless_exit(dag_dir, dag):
    dag.layer(name="foobar", retries=5, retry_unless_exit=2)

    dags.write_dag(dag, dag_dir)

    assert "RETRY foobar 5 UNLESS-EXIT 2" in dagfile_lines(dag_dir)


def test_layer_category(dag_dir, dag):
    dag.layer(name="foobar", category="cat")

    dags.write_dag(dag, dag_dir)

    assert "CATEGORY foobar cat" in dagfile_lines(dag_dir)


def test_layer_priority(dag_dir, dag):
    dag.layer(name="foobar", priority=3)

    dags.write_dag(dag, dag_dir)

    assert "PRIORITY foobar 3" in dagfile_lines(dag_dir)


def test_layer_pre_skip(dag_dir, dag):
    dag.layer(name="foobar", pre_skip_exit_code=1)

    dags.write_dag(dag, dag_dir)

    assert "PRE_SKIP foobar 1" in dagfile_lines(dag_dir)


def test_layer_script_meta(dag_dir, dag):
    dag.layer(
        name="foobar",
        pre=dags.Script(
            executable="/bin/sleep",
            arguments=["5m"],
            retry=True,
            retry_status=2,
            retry_delay=3,
        ),
    )

    dags.write_dag(dag, dag_dir)

    assert "SCRIPT DEFER 2 3 PRE foobar /bin/sleep 5m" in dagfile_lines(dag_dir)


def test_layer_abort(dag_dir, dag):
    dag.layer(name="foobar", abort=dags.DAGAbortCondition(node_exit_value=3))

    dags.write_dag(dag, dag_dir)

    assert "ABORT-DAG-ON foobar 3" in dagfile_lines(dag_dir)


def test_layer_abort_with_meta(dag_dir, dag):
    dag.layer(
        name="foobar",
        abort=dags.DAGAbortCondition(node_exit_value=3, dag_return_value=10),
    )

    dags.write_dag(dag, dag_dir)

    assert "ABORT-DAG-ON foobar 3 RETURN 10" in dagfile_lines(dag_dir)


def test_submit_description_from_file(dag_dir, dag):
    p = Path("here.sub")
    dag.layer(name="foobar", submit_description=p)

    dags.write_dag(dag, dag_dir)

    assert f"JOB foobar {p.absolute().as_posix()}" in dagfile_text(dag_dir)
