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


LAYER_INDEX_NAME = [("foo", 0, "foo:0"), ("foo", 5, "foo:5"), ("foo", 10, "foo:10")]


@pytest.mark.parametrize("layer, index, name", LAYER_INDEX_NAME)
def test_simple_formatter_default_args_work_as_expected(layer, index, name):
    f = dags.SimpleFormatter()

    assert f.generate(layer, index) == name


@pytest.mark.parametrize("layer, index, name", LAYER_INDEX_NAME)
def test_simple_formatter_can_parse_as_expected(layer, index, name):
    f = dags.SimpleFormatter()

    assert f.parse(name) == (layer, index)


@pytest.mark.parametrize("layer, index, name", LAYER_INDEX_NAME)
def test_simple_formatter_is_invertible(layer, index, name):
    f = dags.SimpleFormatter()

    assert f.parse(f.generate(layer, index)) == (layer, index)
    assert f.generate(*f.parse(name)) == name


def test_simple_formatter_layer_name_cant_contain_separator():
    f = dags.SimpleFormatter()

    with pytest.raises(dags.exceptions.LayerNameContainsSeparator):
        f.generate("foo:bar", 0)
