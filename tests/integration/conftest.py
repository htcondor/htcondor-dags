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


@pytest.fixture(scope="function")
def dag_dir(tmp_path):
    d = tmp_path / "dag-dir"
    d.mkdir()

    return d


def dagfile_text(dag_dir, dag_file_name=None):
    if dag_file_name is None:
        dag_file_name = dags.DEFAULT_DAG_FILE_NAME
    text = (dag_dir / dag_file_name).read_text()
    print(text)
    return text


def dagfile_lines(dag_dir, dag_file_name=None):
    return dagfile_text(dag_dir, dag_file_name).splitlines()


@pytest.fixture(scope="function")
def dag():
    return dags.DAG()
