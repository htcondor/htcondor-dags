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

from typing import Optional, MutableMapping, List, Dict, Iterable, Union, Iterator
import logging

import itertools
import collections
from pathlib import Path

from . import dag, exceptions

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SEPARATOR = ":"
DAG_FILE_NAME = "dagfile.dag"
CONFIG_FILE_NAME = "dagman.config"
NOOP_SUBMIT_FILE_NAME = "__JOIN__.sub"


class DAGWriter:
    """Not re-entrant!"""

    def __init__(self, dag: "dag.DAG", path: Path, dag_file_name=None):
        self.dag = dag
        self.path = path

        self.dag_file_name = dag_file_name or DAG_FILE_NAME

        self.join_counter = itertools.count()
        self.has_written_noop_file = False

    def write(self):
        self.path.mkdir(parents=True, exist_ok=True)

        self.write_dag_file()
        self.write_submit_files_for_layers()

    def write_dag_file(self):
        with (self.path / self.dag_file_name).open(mode="w") as f:
            for line in self.yield_dag_file_lines():
                f.write(line + "\n")

    def write_submit_files_for_layers(self):
        for layer in (n for n in self.dag.nodes if isinstance(n, dag.NodeLayer)):
            text = str(layer.submit_description) + "\nqueue"
            (self.path / f"{layer.name}.sub").write_text(text)

    def write_noop_submit_file(self):
        """
        Write out the shared submit file for the NOOP join nodes.
        This is not done by default; it is only done if we actually need a
        join node.
        """
        if not self.has_written_noop_file:
            (self.path / NOOP_SUBMIT_FILE_NAME).touch(exist_ok=True)
            self.has_written_noop_file = True

    def yield_dag_file_lines(self) -> Iterator[str]:
        yield "# BEGIN META"
        for line in itertools.chain(self.yield_dag_meta_lines()):
            yield line
        yield "# END META"

        yield "# BEGIN NODES AND EDGES"
        for node in self.dag.walk(order=dag.WalkOrder.BREADTH_FIRST):
            yield from itertools.chain(
                self.yield_node_lines(node), self.yield_edge_lines(node)
            )
        yield "# END NODES AND EDGES"

    def yield_dag_meta_lines(self):
        if len(self.dag.dagman_config) > 0:
            self.write_dagman_config_file()
            yield f"CONFIG {CONFIG_FILE_NAME}"

        if self.dag.jobstate_log is not None:
            yield f"JOBSTATE_LOG {self.dag.jobstate_log.as_posix()}"

        if self.dag.node_status_file is not None:
            nsf = self.dag.node_status_file
            parts = ["NODE_STATUS_FILE", nsf.path.as_posix()]
            if nsf.update_time is not None:
                parts.append(str(nsf.update_time))
            if nsf.always_update:
                parts.append("ALWAYS-UPDATE")
            yield " ".join(parts)

        if self.dag.dot_config is not None:
            c = self.dag.dot_config
            parts = [
                "DOT",
                c.path.as_posix(),
                "UPDATE" if c.update else "DONT-UPDATE",
                "OVERWRITE" if c.overwrite else "DONT-OVERWRITE",
            ]
            if c.include_file is not None:
                parts.extend(("INCLUDE", c.include_file.as_posix()))
            yield " ".join(parts)

        for k, v in self.dag.dagman_job_attrs.items():
            yield f"SET_JOB_ATTR {k} = {v}"

        for category, value in self.dag.max_jobs_per_category.items():
            yield f"CATEGORY {category} {value}"

    def write_dagman_config_file(self):
        contents = "\n".join(f"{k} = {v}" for k, v in self.dag.dagman_config.items())
        (self.path / CONFIG_FILE_NAME).write_text(contents)

    def yield_node_lines(self, node: "dag.BaseNode") -> Iterator[str]:
        if isinstance(node, dag.NodeLayer):
            yield from self.yield_layer_lines(node)
        elif isinstance(node, dag.SubDAG):
            yield from self.yield_subdag_lines(node)

    def yield_layer_lines(self, layer: "dag.NodeLayer") -> Iterator[str]:
        node_meta_parts = self.get_node_meta_parts(layer)

        # write out each low-level dagman node in the layer
        for idx, vars in enumerate(layer.vars):
            name = self.get_node_name(layer, idx)
            parts = [f"JOB {name} {layer.name}.sub"] + node_meta_parts
            yield " ".join(parts)

            if len(vars) > 0:
                parts = [f"VARS {name}"]
                for key, value in vars.items():
                    value_text = str(value).replace("\\", "\\\\").replace('"', r"\"")
                    parts.append(f'{key}="{value_text}"')
                yield " ".join(parts)

            yield from self.yield_node_meta_lines(layer, name)

    def yield_subdag_lines(self, subdag: "dag.SubDAG") -> Iterator[str]:
        parts = [f"SUBDAG EXTERNAL {subdag.name} {subdag.dag_file}"]
        parts += self.get_node_meta_parts(subdag)
        yield " ".join(parts)

        yield from self.yield_node_meta_lines(subdag, subdag.name)

    def get_node_meta_parts(self, node: "dag.BaseNode") -> List[str]:
        parts = []
        if node.dir is not None:
            parts.extend(("DIR", str(node.dir)))
        if node.noop:
            parts.append("NOOP")
        if node.done:
            parts.append("DONE")
        return parts

    def yield_node_meta_lines(self, node: "dag.BaseNode", name: str) -> Iterator[str]:
        if node.retries is not None:
            parts = [f"RETRY {name} {node.retries}"]
            if node.retry_unless_exit is not None:
                parts.append(f"UNLESS-EXIT {node.retry_unless_exit}")
            yield " ".join(parts)

        if node.pre is not None:
            yield from self.yield_script_line(name, node.pre, "PRE")
        if node.post is not None:
            yield from self.yield_script_line(name, node.post, "POST")

        if node.pre_skip_exit_code is not None:
            yield f"PRE_SKIP {name} {node.pre_skip_exit_code}"

        if node.priority != 0:
            yield f"PRIORITY {name} {node.priority}"

        if node.category is not None:
            yield f"CATEGORY {name} {node.category}"

        if node.abort is not None:
            parts = [f"ABORT-DAG-ON {name} {node.abort.node_exit_value}"]
            if node.abort.dag_return_value is not None:
                parts.append(f"RETURN {node.abort.dag_return_value}")
            yield " ".join(parts)

    def yield_script_line(
        self, name: str, script: "dag.Script", which: str
    ) -> Iterator[str]:
        parts = ["SCRIPT"]

        if script.retry:
            parts.extend(("DEFER", script.retry_status, script.retry_delay))

        parts.extend((which.upper(), name, script.executable, *script.arguments))

        yield " ".join(str(p) for p in parts)

    def get_node_name(self, node: "dag.BaseNode", idx: int) -> str:
        if isinstance(node, dag.SubDAG):
            return node.name
        elif isinstance(node, dag.NodeLayer) and len(node.vars) == 1:
            return node.name
        elif isinstance(node, dag.NodeLayer):
            return f"{node.name}{SEPARATOR}{node.postfix_format.format(idx)}"
        else:
            raise Exception(
                f"Was not able to generate a node name for node {node}, index {idx}"
            )

    def get_indexes_to_node_names(self, node) -> Dict[int, str]:
        if isinstance(node, dag.SubDAG):
            return {0: node.name}
        elif isinstance(node, dag.NodeLayer):
            return {idx: self.get_node_name(node, idx) for idx in range(len(node.vars))}
        else:
            raise Exception(f"Was not able to generate a node names for node {node}")

    def yield_edge_lines(self, node: "dag.BaseNode") -> Iterator[str]:
        parents = self.get_indexes_to_node_names(node)
        for child in node.children:
            children = self.get_indexes_to_node_names(child)

            edge_type = self.dag._edges.get(node, child)
            if isinstance(edge_type, dag.ManyToMany):
                if len(parents) == 1 or len(children) == 1:
                    yield f"PARENT {' '.join(parents.values())} CHILD {' '.join(children.values())}"
                else:
                    self.write_noop_submit_file()
                    join_name = f"__JOIN__{SEPARATOR}{next(self.join_counter)}"
                    yield f"JOB {join_name} {NOOP_SUBMIT_FILE_NAME} NOOP"
                    yield f"PARENT {' '.join(parents.values())} CHILD {join_name}"
                    yield f"PARENT {join_name} CHILD {' '.join(children.values())}"
            elif isinstance(edge_type, dag.OneToOne):
                if len(parents) != len(children):
                    raise exceptions.OneToOneEdgeNeedsSameNumberOfVars(
                        f"parent layer {node} has {len(parents)} nodes, but child layer {child} has {len(children)} nodes"
                    )
                for (parent, child) in zip(parents.values(), children.values()):
                    yield f"PARENT {parent} CHILD {child}"
            else:
                raise NotImplementedError("No support for generic Edges yet!")
                parent_to_children = collections.defaultdict(set)
                for parent_idx in parents:
                    for child_idx in children:
                        if edge_type.is_edge(parent_idx, child_idx):
                            parent_to_children[parent_idx].add(child_idx)

                parent_to_children = {
                    k: tuple(sorted(v)) for k, v in parent_to_children.items()
                }

                children_to_parents = collections.defaultdict(set)
                for parent, children in parent_to_children.items():
                    children_to_parents[children].add(parent)

                edges = {tuple(sorted(v)): k for k, v in children_to_parents}
                for parent_idxs, child_idxs in edges.items():
                    raise Exception
