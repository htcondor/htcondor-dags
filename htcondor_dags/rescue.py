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

import logging
from typing import Optional, List, Dict, Iterator, Mapping

import collections
from pathlib import Path

from .formatter import SimpleFormatter


def rescue(dag, rescue_file, formatter=None):
    return _rescue(dag, Path(rescue_file).read_text(), formatter)


def _rescue(dag, rescue_file_text, formatter=None):
    if formatter is None:
        formatter = SimpleFormatter()

    finished_nodes = parse_rescue_file_text(rescue_file_text, formatter)

    apply_rescue(dag, finished_nodes)


# TODO: doesn't work for subdag nodes!
def parse_rescue_file_text(rescue_file_text, formatter):
    finished_nodes = collections.defaultdict(set)
    for line in rescue_file_text.splitlines():
        if line.startswith("#"):
            continue
        if line == "":
            continue

        print(line)
        node_name = line.lstrip("DONE ")
        print(node_name)
        layer, index = formatter.parse(node_name)
        finished_nodes[layer].add(index)

    return finished_nodes


def apply_rescue(dag, finished_nodes):
    for node in dag.nodes:
        node.done = {}
        for index in finished_nodes[node.name]:
            node.done[index] = True
