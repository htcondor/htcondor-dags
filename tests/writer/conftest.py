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
from htcondor_dags.writer import DAGWriter


@pytest.fixture(scope="function")
def writer(dag):
    return DAGWriter(dag)


@pytest.fixture(scope="function")
def dag_dir(tmp_path):
    d = tmp_path / "dag-dir"
    d.mkdir()

    return d


def dagfile_lines(writer):
    lines = list(writer.yield_dag_file_lines())
    print("\n" + " DAGFILE LINES ".center(19, "-"))
    for line in lines:
        print(line)
    print("-" * 20)
    return lines


def dagfile_text(writer):
    return "\n".join(dagfile_lines(writer))
