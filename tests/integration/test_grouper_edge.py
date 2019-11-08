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
from tests.integration.conftest import dagfile_lines


def test_grouper_edge_produces_correct_dagfile_lines(dag_dir, dag):
    parent = dag.layer(name="parent", vars=[{}] * 6)
    child = parent.child_layer(name="child", vars=[{}] * 4, edge=dags.Grouper(3, 2))

    dags.write_dag(dag, dag_dir)

    lines = dagfile_lines(dag_dir)

    assert (
        f"PARENT parent{dags.SEPARATOR}0 parent{dags.SEPARATOR}1 parent{dags.SEPARATOR}2 CHILD __JOIN__{dags.SEPARATOR}0"
        in lines
    )
    assert (
        f"PARENT parent{dags.SEPARATOR}3 parent{dags.SEPARATOR}4 parent{dags.SEPARATOR}5 CHILD __JOIN__{dags.SEPARATOR}1"
        in lines
    )
    assert (
        f"PARENT __JOIN__{dags.SEPARATOR}0 CHILD child{dags.SEPARATOR}0 child{dags.SEPARATOR}1"
        in lines
    )
    assert (
        f"PARENT __JOIN__{dags.SEPARATOR}1 CHILD child{dags.SEPARATOR}2 child{dags.SEPARATOR}3"
        in lines
    )


@pytest.mark.parametrize(
    "num_parent_vars, num_child_vars, parent_group_size, child_group_size",
    [
        (6, 4, 3, 2),
        (9, 3, 3, 1),
        (9, 6, 3, 2),
        (1, 1, 1, 1),
        (2, 1, 2, 1),
        (1, 2, 1, 2),
        (2, 2, 1, 1),
    ],
)
def test_compatible_grouper_edges(
    num_parent_vars, num_child_vars, parent_group_size, child_group_size, dag, dag_dir
):
    parent = dag.layer(name="parent", vars=[{}] * num_parent_vars)
    child = parent.child_layer(
        name="child",
        vars=[{}] * num_child_vars,
        edge=dags.Grouper(parent_group_size, child_group_size),
    )

    dags.write_dag(dag, dag_dir)


@pytest.mark.parametrize(
    "num_parent_vars, num_child_vars, parent_group_size, child_group_size",
    [(2, 1, 1, 1), (1, 2, 1, 1), (9, 6, 3, 3), (5, 1, 3, 1), (1, 5, 1, 3)],
)
def test_incompatible_grouper_edges(
    num_parent_vars, num_child_vars, parent_group_size, child_group_size, dag, dag_dir
):
    parent = dag.layer(name="parent", vars=[{}] * num_parent_vars)
    child = parent.child_layer(
        name="child",
        vars=[{}] * num_child_vars,
        edge=dags.Grouper(parent_group_size, child_group_size),
    )

    with pytest.raises(dags.exceptions.IncompatibleGrouper):
        dags.write_dag(dag, dag_dir)
