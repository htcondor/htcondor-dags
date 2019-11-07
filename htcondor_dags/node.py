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


from typing import Optional, Dict, Iterable, Union, List, Iterator, Mapping
import logging

import itertools
import functools
from pathlib import Path
import abc

import htcondor

from . import dag, edges, utils, exceptions
from .walk_order import WalkOrder


class DAGAbortCondition:
    def __init__(self, node_exit_value: int, dag_return_value: Optional[int] = None):
        self.node_exit_value = node_exit_value
        self.dag_return_value = dag_return_value

    def __repr__(self):
        return utils.make_repr(self, ("node_exit_value", "dag_return_value"))


class Script:
    def __init__(
        self,
        executable: Union[str, Path],
        arguments: Optional[List[str]] = None,
        retry: bool = False,
        retry_status: int = 1,
        retry_delay: int = 0,
    ):
        """
        Parameters
        ----------
        executable
            The path to the executable to run.
        arguments
            The individual arguments to the executable. Keep in mind that these
            are evaluated as soon as the :class:`Script` is created!
        retry
            ``True`` if the script can be retried on failure.
        retry_status
            If the script exits with this status, the script run will be
            considered a failure for the purposes of retrying.
        retry_delay
            The number of seconds to wait after a script failure before
            retrying.
        """
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


@functools.total_ordering
class BaseNode(abc.ABC):
    def __init__(
        self,
        dag,
        *,
        name: str,
        dir: Optional[Path] = None,
        noop: Union[bool, Mapping[int, bool]] = False,
        done: Union[bool, Mapping[int, bool]] = False,
        retries: Optional[int] = None,
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

    def __repr__(self) -> str:
        return utils.make_repr(self, ("name",))

    def description(self) -> str:
        data = "\n".join(f"  {k} = {v}" for k, v in self.__dict__.items())
        return f"{self.__class__.__name__}(\n{data}\n)"

    def __iter__(self) -> "BaseNode":
        yield self

    def __hash__(self):
        return hash((self.__class__, self.name))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self._dag == other._dag and self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, NodeLayer):
            return NotImplemented
        return self.name < other.name

    def child_layer(
        self, edge: Optional[edges.BaseEdge] = None, **kwargs
    ) -> "NodeLayer":
        node = self._dag.layer(**kwargs)

        self._dag._edges.add(self, node, edge=edge)

        return node

    def parent_layer(
        self, edge: Optional[edges.BaseEdge] = None, **kwargs
    ) -> "NodeLayer":
        node = self._dag.layer(**kwargs)

        self._dag._edges.add(node, self, edge=edge)

        return node

    def child_subdag(self, edge: Optional[edges.BaseEdge] = None, **kwargs) -> "SubDAG":
        node = self._dag.subdag(**kwargs)

        self._dag._edges.add(self, node, edge=edge)

        return node

    def parent_subdag(
        self, edge: Optional[edges.BaseEdge] = None, **kwargs
    ) -> "SubDAG":
        node = self._dag.subdag(**kwargs)

        self._dag._edges.add(node, self, edge=edge)

        return node

    def add_children(self, *nodes, edge: Optional[edges.BaseEdge] = None) -> "BaseNode":
        nodes = utils.flatten(nodes)
        for node in nodes:
            self._dag._edges.add(self, node, edge=edge)

        return self

    def remove_children(self, *nodes) -> "BaseNode":
        nodes = utils.flatten(nodes)
        for node in nodes:
            self._dag._edges.remove(self, node)

        return self

    def add_parents(self, *nodes, edge: Optional[edges.BaseEdge] = None) -> "BaseNode":
        nodes = utils.flatten(nodes)
        for node in nodes:
            self._dag._edges.add(node, self, edge=edge)

        return self

    def remove_parents(self, *nodes) -> "BaseNode":
        nodes = utils.flatten(nodes)
        for node in nodes:
            self._dag._edges.remove(node, self)

        return self

    @property
    def parents(self) -> "Nodes":
        return self._dag.node_to_parents[self]

    @property
    def children(self) -> "Nodes":
        return self._dag.node_to_children[self]

    def walk_ancestors(
        self, order: WalkOrder = WalkOrder.DEPTH_FIRST
    ) -> Iterator["BaseNode"]:
        return self._dag.walk_ancestors(node=self, order=order)

    def walk_descendants(
        self, order: WalkOrder = WalkOrder.DEPTH_FIRST
    ) -> Iterator["BaseNode"]:
        return self._dag.walk_descendants(node=self, order=order)


class NodeLayer(BaseNode):
    def __init__(
        self,
        dag: "dag.DAG",
        *,
        postfix_format="{:d}",
        submit_description: Union[Optional[htcondor.Submit], Path] = None,
        vars: Optional[Iterable[Dict[str, str]]] = None,
        **kwargs,
    ):
        super().__init__(dag, **kwargs)

        self.postfix_format = postfix_format

        self.submit_description = submit_description or htcondor.Submit({})

        # todo: this is bad, should be an empty list
        if vars is None:
            vars = [{}]
        self.vars = list(vars)


class SubDAG(BaseNode):
    def __init__(self, dag: "dag.DAG", *, dag_file: Path, **kwargs):
        super().__init__(dag, **kwargs)

        self.dag_file = dag_file


class FinalNode(BaseNode):
    def __init__(
        self,
        dag: "dag.DAG",
        submit_description: Union[Optional[htcondor.Submit], Path] = None,
        **kwargs,
    ):
        super().__init__(dag, **kwargs)

        self.submit_description = submit_description or htcondor.Submit({})


class Nodes:
    def __init__(self, *nodes):
        self.nodes = dag.NodeStore()
        nodes = utils.flatten(nodes)
        for node in nodes:
            self.nodes.add(node)

    def __eq__(self, other):
        return self.nodes == other.nodes

    def __len__(self) -> int:
        return len(self.nodes)

    def __iter__(self) -> Iterator[BaseNode]:
        yield from self.nodes

    def __contains__(self, node) -> bool:
        return node in self.nodes

    def __repr__(self) -> str:
        return f"Nodes({', '.join(repr(n) for n in sorted(self.nodes, key = lambda n: n.name))})"

    def __str__(self) -> str:
        return f"Nodes({', '.join(str(n) for n in sorted(self.nodes, key = lambda n: n.name))})"

    def _some_element(self) -> BaseNode:
        return next(iter(self.nodes))

    def child_layer(self, type: Optional[edges.BaseEdge] = None, **kwargs) -> NodeLayer:
        node = self._some_element().child_layer(**kwargs)

        node.add_parents(self, edge=type)

        return node

    def parent_layer(
        self, type: Optional[edges.BaseEdge] = None, **kwargs
    ) -> NodeLayer:
        node = self._some_element().parent_layer(**kwargs)

        node.add_children(self, edge=type)

        return node

    def child_subdag(self, type: Optional[edges.BaseEdge] = None, **kwargs) -> SubDAG:
        node = self._some_element().child_subdag(**kwargs)

        node.add_parents(self, edge=type)

        return node

    def parent_subdag(self, type: Optional[edges.BaseEdge] = None, **kwargs) -> SubDAG:
        node = self._some_element().parent_subdag(**kwargs)

        node.add_children(self, edge=type)

        return node

    def add_children(self, *nodes, type: Optional[edges.BaseEdge] = None):
        for s in self:
            s.add_children(nodes, edge=type)

    def remove_children(self, *nodes):
        for s in self:
            s.remove_children(nodes)

    def add_parents(self, *nodes, type: Optional[edges.BaseEdge] = None):
        for s in self:
            s.add_parents(nodes, edge=type)

    def remove_parents(self, *nodes):
        for s in self:
            s.remove_parents(nodes)

    def walk_ancestors(self, order: WalkOrder = WalkOrder.DEPTH_FIRST):
        return itertools.chain.from_iterable(
            n.walk_ancestors(order=order) for n in self
        )

    def walk_descendants(self, order: WalkOrder = WalkOrder.DEPTH_FIRST):
        return itertools.chain.from_iterable(
            n.walk_descendants(order=order) for n in self
        )
