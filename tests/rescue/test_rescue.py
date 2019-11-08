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
from htcondor_dags.rescue import _rescue


def test_rescue(rescue_dag, rescue_file_text):
    _rescue(rescue_dag, rescue_file_text)

    assert rescue_dag._nodes["a"].done == {0: True}
    assert rescue_dag._nodes["b"].done == {0: True}
    assert rescue_dag._nodes["c"].done == {}
    assert rescue_dag._nodes["d"].done == {}
