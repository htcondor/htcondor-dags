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


def test_walk_depth_first(dag):
    a = dag.layer(name="a")
    b = a.child_layer(name="b")
    c = a.child_layer(name="c")
    d = dag.layer(name="d")
    d.add_parents(b, c)

    nodes = list(dag.walk(dags.WalkOrder.DEPTH_FIRST))
    # sibling order is not specified
    assert nodes in ([a, b, d, c], [a, c, d, b])


def test_walk_breadth_first(dag):
    a = dag.layer(name="a")
    b = a.child_layer(name="b")
    c = a.child_layer(name="c")
    d = dag.layer(name="d")
    d.add_parents(b, c)

    nodes = list(dag.walk(dags.WalkOrder.BREADTH_FIRST))
    # sibling order is not specified
    assert nodes in ([a, b, c, d], [a, c, b, d])


def test_walk_bad_order_raises(dag):
    a = dag.layer(name="a")
    b = a.child_layer(name="b")
    c = a.child_layer(name="c")
    d = dag.layer(name="d")
    d.add_parents(b, c)

    with pytest.raises(dags.exceptions.UnrecognizedWalkOrder):
        list(dag.walk(order="foobar"))
