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

from typing import Optional, Dict, Iterable, Union, List, Any, Iterator
import logging

import collections
import itertools
import functools
import enum
from pathlib import Path
import collections.abc
import fnmatch
import abc

import htcondor

from . import writer, utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DotConfig:
    def __init__(
        self,
        path: utils.Openable,
        update: bool = False,
        overwrite: bool = True,
        include_file: Optional[utils.Openable] = None,
    ):
        self.path = Path(path)
        self.update = update
        self.overwrite = overwrite
        self.include_file = include_file if include_file is None else Path(include_file)

    def __repr__(self):
        return utils.make_repr(self, ("path", "update", "overwrite", "include_file"))


class NodeStatusFile:
    def __init__(self, path: utils.Openable, update_time=None, always_update=False):
        self.path = Path(path)
        self.update_time = update_time
        self.always_update = always_update

    def __repr__(self):
        return utils.make_repr(self, ("path", "update_time", "always_update"))


class WalkOrder(enum.Enum):
    DEPTH_FIRST = "DEPTH"
    BREADTH_FIRST = "BREADTH"


class DAG:
    def __init__(
        self,
        jobstate_log: Optional[utils.Openable] = None,
        max_jobs_by_category: Optional[Dict[str, int]] = None,
        dagman_config: Optional[Dict[str, Any]] = None,
        dagman_job_attributes: Optional[Dict[str, Any]] = None,
        dot_config: Optional[DotConfig] = None,
        node_status_file: Optional[NodeStatusFile] = None,
    ):
        self._nodes = NodeStore()
        self._edges = EdgeStore()
        self.jobstate_log = jobstate_log if jobstate_log is None else Path(jobstate_log)
        self.max_jobs_per_category = max_jobs_by_category or {}
        self.dagman_config = dagman_config or {}
        self.dagman_job_attrs = dagman_job_attributes or {}
        self.dot_config = dot_config
        self.node_status_file = node_status_file

    @property
    def nodes(self):
        return self._nodes

    @property
    def edges(self):
        return self._edges

    def __contains__(self, node):
        return node in self._nodes

    def layer(self, **kwargs):
        node = NodeLayer(dag=self, **kwargs)
        self.nodes.add(node)
        return node

    def subdag(self, **kwargs):
        node = SubDAG(dag=self, **kwargs)
        self.nodes.add(node)
        return node

    def select(self, pattern: str):
        return Nodes(
            *(
                node
                for name, node in self._nodes.items()
                if fnmatch.fnmatchcase(name, pattern)
            )
        )

    @property
    def node_to_children(self):
        d = {n: set() for n in self.nodes}
        for parent, child in self.edges:
            d[parent].add(child)

        return {k: Nodes(v) for k, v in d.items()}

    @property
    def node_to_parents(self):
        d = {n: set() for n in self.nodes}
        for parent, child in self.edges:
            d[child].add(parent)

        return {k: Nodes(v) for k, v in d.items()}

    def roots(self):
        return Nodes(
            child
            for child, parents in self.node_to_parents.items()
            if len(parents) == 0
        )

    def walk(self, order: WalkOrder = WalkOrder.DEPTH_FIRST) -> Iterator["BaseNode"]:
        seen = set()
        stack = collections.deque(self.roots())

        while len(stack) != 0:
            if order is WalkOrder.DEPTH_FIRST:
                node = stack.pop()
            elif order is WalkOrder.BREADTH_FIRST:
                node = stack.popleft()
            else:
                raise Exception(f"Unrecognized {WalkOrder.__name__}")

            if node in seen:
                continue
            seen.add(node)

            stack.extend(node.children)
            yield node

    def write(self, dag_dir: utils.Openable):
        return writer.DAGWriter(self, dag_dir).write()


class DAGAbortCondition:
    def __init__(self, node_exit_value: int, dag_return_value: Optional[int] = None):
        self.node_exit_value = node_exit_value
        self.dag_return_value = dag_return_value

    def __repr__(self):
        return utils.make_repr(self, ("node_exit_value", "dag_return_value"))


class Script:
    def __init__(
        self,
        executable,
        arguments: Optional[List[str]] = None,
        retry: bool = False,
        retry_status: int = 1,
        retry_delay: int = 0,
    ):
        self.executable = executable
        if arguments is None:
            arguments = []
        self.arguments = [str(arg) for arg in arguments]

        self.retry = retry
        self.retry_status = retry_status
        self.retry_delay = retry_delay

    def __repr__(self):
        return utils.make_repr(
            self, ("executable", "arguments", "retry", "retry_status", "retry_delay")
        )


class EdgeType(abc.ABC):
    def is_edge(self, parent_index, child_index) -> bool:
        raise NotImplementedError


class ManyToMany(EdgeType):
    def is_edge(self, parent_index, child_index) -> bool:
        return True


class OneToOne(EdgeType):
    def is_edge(self, parent_index, child_index) -> bool:
        return parent_index == child_index


class EdgeStore:
    def __init__(self):
        self.edges = {}

    def __iter__(self):
        yield from self.edges

    def __contains__(self, item):
        return item in self.edges

    def get(self, parent, child):
        try:
            return self.edges[(parent, child)]
        except KeyError:
            return None

    def add(self, parent, child, type=None):
        if type is None:
            type = ManyToMany()
        self.edges[(parent, child)] = type

    def pop(self, parent, child):
        return self.edges.pop((parent, child), None)


class NodeStore:
    def __init__(self):
        self.nodes = {}

    def add(self, *nodes):
        for node in nodes:
            if isinstance(node, BaseNode):
                self.nodes[node.name] = node
            elif isinstance(node, Nodes):
                self.add(self, node)

    def remove(self, *nodes):
        for node in nodes:
            if isinstance(node, str):
                self.nodes.pop(node, None)
            elif isinstance(node, BaseNode):
                self.nodes.pop(node.name, None)
            elif isinstance(node, Nodes):
                self.remove(node)

    def __getitem__(self, node):
        if isinstance(node, str):
            return self.nodes[node]
        elif isinstance(node, BaseNode):
            return self.nodes[node.name]
        else:
            raise KeyError()

    def __iter__(self):
        yield from self.nodes.values()

    def __contains__(self, node):
        if isinstance(node, BaseNode):
            return node in self.nodes.values()
        elif isinstance(node, str):
            return node in self.nodes.keys()
        return False

    def items(self):
        yield from self.nodes.items()

    def __repr__(self):
        return repr(set(self.nodes.values()))

    def __str__(self):
        return str(set(self.nodes.values()))

    def __len__(self):
        return len(self.nodes)


def flatten(nested_iterable) -> List[Any]:
    return list(itertools.chain.from_iterable(nested_iterable))


@functools.total_ordering
class BaseNode(abc.ABC):
    def __init__(
        self,
        dag,
        *,
        name: str,
        dir: Optional[utils.Openable] = None,
        noop: bool = False,
        done: bool = False,
        retries: Optional[int] = 0,
        retry_unless_exit: Optional[int] = None,
        pre: Optional[Script] = None,
        pre_skip_exit_code=None,
        post: Optional[Script] = None,
        priority: int = 0,
        category: Optional[str] = None,
        abort: Optional[DAGAbortCondition] = None,
    ):
        self._dag = dag
        self.name = name

        self.dir = Path(dir) if dir is not None else None
        self.noop = noop
        self.done = done

        self.retries = retries
        self.retry_unless_exit = retry_unless_exit
        self.priority = priority
        self.category = category
        self.abort = abort

        self.pre = pre
        self.pre_skip_exit_code = pre_skip_exit_code
        self.post = post

    def __repr__(self):
        return utils.make_repr(self, ("name",))

    def description(self):
        data = "\n".join(f"  {k} = {v}" for k, v in self.__dict__.items())
        return f"{self.__class__.__name__}(\n{data}\n)"

    def __iter__(self):
        yield self

    def __hash__(self):
        return hash((self.__class__, self.name))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, NodeLayer):
            return NotImplemented
        return self.name < other.name

    def child(self, type=None, **kwargs):
        node = self._dag.layer(**kwargs)

        self._dag.edges.add(self, node, type=None)

        return node

    def parent(self, type=None, **kwargs):
        node = self._dag.layer(**kwargs)

        self._dag.edges.add(node, self, type=None)

        return node

    def add_children(self, *nodes, type=None):
        nodes = flatten(nodes)
        for node in nodes:
            self._dag.edges.add(self, node, type=type)

        return self

    def remove_children(self, *nodes):
        nodes = flatten(nodes)
        for node in nodes:
            self._dag.edges.remove(self, node)

        return self

    def add_parents(self, *nodes, type=None):
        nodes = flatten(nodes)
        for node in nodes:
            self._dag.edges.add(node, self, type=type)

        return self

    def remove_parents(self, *nodes):
        nodes = flatten(nodes)
        for node in nodes:
            self._dag.edges.remove(node, self)

        return self

    @property
    def parents(self):
        return self._dag.node_to_parents[self]

    @property
    def children(self):
        return self._dag.node_to_children[self]


class NodeLayer(BaseNode):
    def __init__(
        self,
        dag: DAG,
        *,
        postfix_format="{:d}",
        submit_description: Optional[htcondor.Submit] = None,
        vars: Optional[Iterable[Dict[str, str]]] = None,
        **kwargs,
    ):
        super().__init__(dag, **kwargs)

        self.postfix_format = postfix_format

        self.submit_description = submit_description or htcondor.Submit({})

        if vars is None:
            vars = [{}]
        self.vars = list(vars)


class SubDAG(BaseNode):
    def __init__(self, dag: DAG, *, dag_file: Path, **kwargs):
        super().__init__(dag, **kwargs)

        self.dag_file = dag_file


class Nodes:
    def __init__(self, *nodes):
        self.nodes = NodeStore()
        nodes = flatten(nodes)
        for node in nodes:
            self.nodes.add(node)

    def __len__(self):
        return len(self.nodes)

    def __iter__(self):
        yield from self.nodes

    def __contains__(self, node):
        return node in self.nodes

    def __repr__(self):
        return f"Nodes({', '.join(repr(n) for n in sorted(self.nodes, key = lambda n: n.name))})"

    def __str__(self):
        return f"Nodes({', '.join(str(n) for n in sorted(self.nodes, key = lambda n: n.name))})"

    def _some_element(self):
        return next(iter(self.nodes))

    def child(self, type=None, **kwargs):
        node = self._some_element().child(**kwargs)

        node.add_parents(self, type=type)

        return node

    def parent(self, type=None, **kwargs):
        node = self._some_element().parent(**kwargs)

        node.add_children(self, type=type)

        return node

    def add_children(self, *nodes, type=None):
        for s in self:
            s.add_children(nodes, type=type)

    def remove_children(self, *nodes):
        for s in self:
            s.remove_children(nodes)

    def add_parents(self, *nodes, type=None):
        for s in self:
            s.add_parents(nodes, type=type)

    def remove_parents(self, *nodes):
        for s in self:
            s.remove_parents(nodes)
