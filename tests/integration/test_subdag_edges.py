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


def test_one_parent_one_child(dag_dir, dag):
    parent = dag.subdag(name="parent", dag_file="parent.dag")
    child = parent.child_subdag(name="child", dag_file="child.dag")

    dag.write(dag_dir)

    assert "PARENT parent CHILD child" in dagfile_lines(dag_dir)


def test_two_parents_one_child(dag_dir, dag):
    parent1 = dag.subdag(name="parent1", dag_file="parent.dag")
    parent2 = dag.subdag(name="parent2", dag_file="parent.dag")
    child = parent1.child_subdag(name="child", dag_file="child.dag")
    child.add_parents(parent2)

    dag.write(dag_dir)

    lines = dagfile_lines(dag_dir)
    assert f"PARENT parent1 CHILD child" in lines
    assert f"PARENT parent2 CHILD child" in lines


def test_one_parent_two_children(dag_dir, dag):
    parent1 = dag.subdag(name="parent", dag_file="parent.dag")
    child1 = parent1.child_subdag(name="child1", dag_file="child.dag")
    child2 = parent1.child_subdag(name="child2", dag_file="child.dag")

    dag.write(dag_dir)

    lines = dagfile_lines(dag_dir)
    assert f"PARENT parent CHILD child1" in lines
    assert f"PARENT parent CHILD child2" in lines
