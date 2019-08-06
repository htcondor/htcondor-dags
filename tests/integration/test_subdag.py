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

import htcondor_dags as dags
from .conftest import dagfile_lines, dagfile_text


def test_subdag_name_appears(dag_dir, dag):
    dag.subdag(name="foobar", dag_file="subdag.dag")

    dag.write(dag_dir)

    assert "foobar" in dagfile_text(dag_dir)


def test_subdag_line_appears(dag_dir, dag):
    dag.subdag(name="foobar", dag_file="subdag.dag")

    dag.write(dag_dir)

    assert "SUBDAG EXTERNAL foobar subdag.dag" in dagfile_lines(dag_dir)
