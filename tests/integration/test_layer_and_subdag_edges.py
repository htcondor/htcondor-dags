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

import htcondor_dags as dags
from .conftest import dagfile_lines, dagfile_text


def test_one_parent_one_child(dag_dir, dag):
    parent = dag.layer(name="parent")
    child = parent.child_subdag(name="child", dag_file="foobar.dag")

    dag.write(dag_dir)

    assert "PARENT parent CHILD child" in dagfile_lines(dag_dir)


def test_two_parents_one_child(dag_dir, dag):
    parent = dag.layer(name="parent", vars=[{}, {}])
    child = parent.child_subdag(name="child", dag_file="foobar.dag")

    dag.write(dag_dir)

    assert (
        f"PARENT parent{dags.SEPARATOR}0 parent{dags.SEPARATOR}1 CHILD child"
        in dagfile_lines(dag_dir)
    )


def test_one_parent_two_children(dag_dir, dag):
    parent = dag.subdag(name="parent", dag_file="foobar.dag")
    child = parent.child_layer(name="child", vars=[{}, {}])

    dag.write(dag_dir)

    assert (
        f"PARENT parent CHILD child{dags.SEPARATOR}0 child{dags.SEPARATOR}1"
        in dagfile_lines(dag_dir)
    )
