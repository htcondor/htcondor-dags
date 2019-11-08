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
from .writer import DEFAULT_DAG_FILE_NAME
from . import exceptions


def rescue(dag, rescue_file, formatter=None):
    return _rescue(dag, Path(rescue_file).read_text(), formatter)


def _rescue(dag, rescue_file_text, formatter=None):
    if formatter is None:
        formatter = SimpleFormatter()

    finished_nodes = parse_rescue_file_text(rescue_file_text, formatter)

    apply_rescue(dag, finished_nodes)


def parse_rescue_file_text(rescue_file_text, formatter):
    finished_nodes = collections.defaultdict(set)
    for line in rescue_file_text.splitlines():
        if line.startswith("#"):
            continue
        if line == "":
            continue

        node_name = line.lstrip("DONE ")
        layer, index = formatter.parse(node_name)
        finished_nodes[layer].add(index)

    return finished_nodes


def apply_rescue(dag, finished_nodes):
    for node in dag.nodes:
        node.done = {}
        for index in finished_nodes[node.name]:
            node.done[index] = True


def find_rescue_file(
    dag_dir: Path, dag_file_name: str = DEFAULT_DAG_FILE_NAME
) -> Optional[Path]:
    dag_dir = Path(dag_dir)
    rescue_files = sorted(dag_dir.glob(f"{dag_file_name}.rescue*"))

    if len(rescue_files) == 0:
        raise exceptions.NoRescueFileFound(
            f"No rescue file for dag {dag_file_name} found in {dag_dir}"
        )

    return rescue_files[-1]
