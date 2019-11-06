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


def test_can_mark_part_of_layer_noop(dag, dag_dir):
    layer = dag.layer(name="layer", vars=[{}] * 2, noop={0: True})

    dag.write(dag_dir)

    lines = dagfile_lines(dag_dir)
    assert f"JOB layer{dags.SEPARATOR}0 layer.sub NOOP" in lines
    assert f"JOB layer{dags.SEPARATOR}1 layer.sub" in lines


def test_can_mark_part_of_layer_done(dag, dag_dir):
    layer = dag.layer(name="layer", vars=[{}] * 2, done={0: True})

    dag.write(dag_dir)

    lines = dagfile_lines(dag_dir)
    assert f"JOB layer{dags.SEPARATOR}0 layer.sub DONE" in lines
    assert f"JOB layer{dags.SEPARATOR}1 layer.sub" in lines
