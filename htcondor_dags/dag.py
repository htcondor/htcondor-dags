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

from typing import (
    Optional,
    Dict,
    Iterable,
    Union,
    List,
    Any,
    Iterator,
    Callable,
    Tuple,
    TypeVar,
)
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

from . import writer, utils, exceptions

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


def _check_node_name_uniqueness(func):
    def wrapper(dag: "DAG", **kwargs):
        name = kwargs["name"]
        if name in dag._nodes:
            raise exceptions.DuplicateNodeName(
                f"the DAG already has a node named {name}: {dag._nodes[name]}"
            )
        if dag._final_node is not None and dag._final_node.name == name:
            raise exceptions.DuplicateNodeName(
                f"the DAG already has a node named {name}: {dag._final_node}"
            )

        return func(dag, **kwargs)

    return wrapper


class DAG:
    def __init__(
        self,
        dagman_config: Optional[Dict[str, Any]] = None,
        dagman_job_attributes: Optional[Dict[str, Any]] = None,
        max_jobs_by_category: Optional[Dict[str, int]] = None,
        dot_config: Optional[DotConfig] = None,
        jobstate_log: Optional[utils.Openable] = None,
        node_status_file: Optional[NodeStatusFile] = None,
    ):
        """
        Parameters
        ----------
        dagman_config
            A mapping of DAGMan configuration options.
        dagman_job_attributes
            A mapping that describes additional HTCondor JobAd attributes for
            the DAGMan job itself.
        max_jobs_by_category
            A mapping that describes the maximum number of jobs (values) that
            should be run simultaneously from each category (keys).
        dot_config
            A :class:`DotConfig` that tells DAGMan how to write out DOT files
            that describe the state of the DAG.
        jobstate_log
            The path to the jobstate log. If not given, the jobstate log will
            not be written.
        node_status_file
            The path to the node status file. If not given, the node status file
            will not be written.
        """
        self._nodes = NodeStore()
        self._edges = EdgeStore()
        self._final_node = None

        self.jobstate_log = jobstate_log if jobstate_log is None else Path(jobstate_log)
        self.max_jobs_per_category = max_jobs_by_category or {}
        self.dagman_config = dagman_config or {}
        self.dagman_job_attrs = dagman_job_attributes or {}
        self.dot_config = dot_config
        self.node_status_file = node_status_file

    @property
    def nodes(self):
        """Iterate over all of the nodes in the DAG, in no particular order."""
        yield from self._nodes

    def walk(self, order: WalkOrder = WalkOrder.DEPTH_FIRST) -> Iterator["BaseNode"]:
        """
        Iterate over all of the nodes in the DAG, in some sensible order.

        Parameters
        ----------
        order
            Walk depth-first (children before siblings)
            or breadth-first (siblings before children).
        """
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

    @property
    def edges(self) -> Iterator[Tuple[Tuple["BaseNode", "BaseNode"], "EdgeType"]]:
        """Iterate over ((parent, child), edge_type) tuples."""
        yield from self._edges

    def __contains__(self, node) -> bool:
        return node in self._nodes

    @_check_node_name_uniqueness
    def layer(self, **kwargs) -> "NodeLayer":
        """Create a new :class:`NodeLayer` with no parents or children."""
        node = NodeLayer(dag=self, **kwargs)
        self._nodes.add(node)
        return node

    @_check_node_name_uniqueness
    def subdag(self, **kwargs) -> "SubDAG":
        """Create a new :class:`SubDAG` with no parents or children."""
        node = SubDAG(dag=self, **kwargs)
        self._nodes.add(node)
        return node

    @_check_node_name_uniqueness
    def final(self, **kwargs) -> "FinalNode":
        """Create the FINAL node of the DAG."""
        node = FinalNode(dag=self, **kwargs)
        self._final_node = node
        return node

    def select(self, selector: Callable[["BaseNode"], bool]) -> "Nodes":
        """Return a :class:`Nodes` of the nodes in the DAG that satisfy ``selector``."""
        return Nodes(*(node for name, node in self._nodes.items() if selector(node)))

    def glob(self, pattern: str) -> "Nodes":
        """Return a :class:`Nodes` of the nodes in the DAG whose names match the glob ``pattern``."""
        return self.select(lambda node: fnmatch.fnmatchcase(node.name, pattern))

    @property
    def node_to_children(self) -> Dict["BaseNode", "Nodes"]:
        """
        Return a dictionary that maps each node to a :class:`Nodes`
        containing its children.
        The :class:`Nodes` will be empty if the node has no children.
        """
        d = {n: set() for n in self.nodes}
        for parent, child in self.edges:
            d[parent].add(child)

        return {k: Nodes(v) for k, v in d.items()}

    @property
    def node_to_parents(self) -> Dict["BaseNode", "Nodes"]:
        """
        Return a dictionary that maps each node to a :class:`Nodes`
        containing its parents.
        The :class:`Nodes` will be empty if the node has no parents.
        """
        d = {n: set() for n in self.nodes}
        for parent, child in self.edges:
            d[child].add(parent)

        return {k: Nodes(v) for k, v in d.items()}

    def roots(self) -> "Nodes":
        """Return a :class:`Nodes` of the nodes in the DAG that have no parents."""
        return Nodes(
            child
            for child, parents in self.node_to_parents.items()
            if len(parents) == 0
        )

    def leaves(self) -> "Nodes":
        """Return a :class:`Nodes` of the nodes in the DAG that have no children."""
        return Nodes(
            parent
            for parent, children in self.node_to_children.items()
            if len(children) == 0
        )

    def write(
        self, dag_dir: utils.Openable, dag_file_name: Optional[str] = None
    ) -> Path:
        """
        Write out the entire DAG to the given directory.
        This includes the DAG description file itself, as well as any associated
        submit descriptions.

        Parameters
        ----------
        dag_dir
            The directory to write the DAG files to.
        dag_file_name
            The name of the DAG description file itself.

        Returns
        -------
        dag_dir :
            Returns the path to the DAG directory.
        """
        return writer.DAGWriter(self, dag_dir, dag_file_name=dag_file_name).write()

    def describe(self) -> str:
        """Return a tabular description of the DAG's structure."""
        rows = []

        for node in self.walk(WalkOrder.BREADTH_FIRST):
            if isinstance(node, NodeLayer):
                type, name = "Layer", node.name
                vars = len(node.vars)
            elif isinstance(node, SubDAG):
                type, name = "SubDag", node.name
                vars = None
            else:
                raise Exception(f"Unrecognized node type: {node}")

            children = len(node.children)

            if len(node.parents) > 0:
                parents = ", ".join(n.name for n in node.parents)
            else:
                parents = None

            rows.append((type, name, vars, children, parents))

        return utils.table(
            headers=["Type", "Name", "# Nodes", "# Children", "Parents"],
            rows=rows,
            alignment={"Type": "ljust", "Parents": "ljust"},
        )


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


class EdgeType(abc.ABC):
    def is_edge(self, parent_index, child_index) -> bool:
        raise NotImplementedError


class ManyToMany(EdgeType):
    """
    This edge connects two layers "densely": every node in the child layer
    is a child of every node in the parent layer.
    """

    def is_edge(self, parent_index, child_index) -> bool:
        return True


class OneToOne(EdgeType):
    """
    This edge connects two layers "linearly": each underlying node in the child
    layer is a child of the corresponding underlying node with the same index
    in the parent layer.
    """

    def is_edge(self, parent_index, child_index) -> bool:
        return parent_index == child_index


class EdgeStore:
    """
    An EdgeStore stores edges for a DAG.
    """

    def __init__(self):
        self.edges = {}

    def __iter__(self) -> Iterator[EdgeType]:
        yield from self.edges

    def __contains__(self, item) -> bool:
        return item in self.edges

    def items(self) -> Iterator[Tuple[Tuple["BaseNode", "BaseNode"], EdgeType]]:
        yield from self.edges.items()

    def get(self, parent: "BaseNode", child: "BaseNode") -> Optional[EdgeType]:
        try:
            return self.edges[(parent, child)]
        except KeyError:
            return None

    def add(
        self, parent: "BaseNode", child: "BaseNode", type: Optional[EdgeType] = None
    ):
        if type is None:
            type = ManyToMany()
        self.edges[(parent, child)] = type

    def pop(self, parent: "BaseNode", child: "BaseNode") -> Optional[EdgeType]:
        return self.edges.pop((parent, child), None)


class NodeStore:
    """
    A NodeStore behaves roughly like a dictionary mapping node names to nodes.
    However, it does not support setting items. Instead, you can ``add`` or
    ``remove`` nodes from the store. Nodes can be specified by name, or by the
    actual node instance, for flexibility.
    """

    def __init__(self):
        self.nodes = {}

    def add(self, *nodes: "BaseNode"):
        for node in nodes:
            if isinstance(node, BaseNode):
                self.nodes[node.name] = node
            elif isinstance(node, Nodes):
                self.add(self, node)

    def remove(self, *nodes: "BaseNode"):
        for node in nodes:
            if isinstance(node, str):
                self.nodes.pop(node, None)
            elif isinstance(node, BaseNode):
                self.nodes.pop(node.name, None)
            elif isinstance(node, Nodes):
                self.remove(node)

    def __getitem__(self, node: Union["BaseNode", str]):
        if isinstance(node, str):
            return self.nodes[node]
        elif isinstance(node, BaseNode):
            return self.nodes[node.name]
        else:
            raise TypeError(f"Nodes can be retrieved by name or value")

    def __iter__(self) -> Iterator["BaseNode"]:
        yield from self.nodes.values()

    def __contains__(self, node: Union["BaseNode", str]) -> bool:
        if isinstance(node, BaseNode):
            return node in self.nodes.values()
        elif isinstance(node, str):
            return node in self.nodes.keys()
        return False

    def items(self) -> Iterator[Tuple[str, "BaseNode"]]:
        yield from self.nodes.items()

    def __repr__(self) -> str:
        return repr(set(self.nodes.values()))

    def __str__(self) -> str:
        return str(set(self.nodes.values()))

    def __len__(self) -> int:
        return len(self.nodes)


T = TypeVar("T")


def flatten(nested_iterable: Iterator[Iterator[T]]) -> Iterator[T]:
    """Flatt"""
    yield from itertools.chain.from_iterable(nested_iterable)


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
        return self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, NodeLayer):
            return NotImplemented
        return self.name < other.name

    def child(self, type: Optional[EdgeType] = None, **kwargs) -> "NodeLayer":
        node = self._dag.layer(**kwargs)

        self._dag._edges.add(self, node, type=type)

        return node

    def parent(self, type: Optional[EdgeType] = None, **kwargs) -> "NodeLayer":
        node = self._dag.layer(**kwargs)

        self._dag._edges.add(node, self, type=type)

        return node

    def child_subdag(self, type: Optional[EdgeType] = None, **kwargs) -> "SubDAG":
        node = self._dag.subdag(**kwargs)

        self._dag._edges.add(self, node, type=type)

        return node

    def parent_subdag(self, type: Optional[EdgeType] = None, **kwargs) -> "SubDAG":
        node = self._dag.subdag(**kwargs)

        self._dag._edges.add(node, self, type=type)

        return node

    def add_children(self, *nodes, type: Optional[EdgeType] = None) -> "BaseNode":
        nodes = flatten(nodes)
        for node in nodes:
            self._dag._edges.add(self, node, type=type)

        return self

    def remove_children(self, *nodes) -> "BaseNode":
        nodes = flatten(nodes)
        for node in nodes:
            self._dag._edges.remove(self, node)

        return self

    def add_parents(self, *nodes, type: Optional[EdgeType] = None) -> "BaseNode":
        nodes = flatten(nodes)
        for node in nodes:
            self._dag._edges.add(node, self, type=type)

        return self

    def remove_parents(self, *nodes) -> "BaseNode":
        nodes = flatten(nodes)
        for node in nodes:
            self._dag._edges.remove(node, self)

        return self

    @property
    def parents(self) -> "Nodes":
        return self._dag.node_to_parents[self]

    @property
    def children(self) -> "Nodes":
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

        # todo: this is bad, should be an empty list
        if vars is None:
            vars = [{}]
        self.vars = list(vars)


class SubDAG(BaseNode):
    def __init__(self, dag: DAG, *, dag_file: Path, **kwargs):
        super().__init__(dag, **kwargs)

        self.dag_file = dag_file


class FinalNode(BaseNode):
    def __init__(
        self, dag: DAG, submit_description: Optional[htcondor.Submit] = None, **kwargs
    ):
        super().__init__(dag, **kwargs)

        self.submit_description = submit_description or htcondor.Submit({})


class Nodes:
    def __init__(self, *nodes):
        self.nodes = NodeStore()
        nodes = flatten(nodes)
        for node in nodes:
            self.nodes.add(node)

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

    def child(self, type: Optional[EdgeType] = None, **kwargs) -> NodeLayer:
        node = self._some_element().child(**kwargs)

        node.add_parents(self, type=type)

        return node

    def parent(self, type: Optional[EdgeType] = None, **kwargs) -> NodeLayer:
        node = self._some_element().parent(**kwargs)

        node.add_children(self, type=type)

        return node

    def child_subdag(self, type: Optional[EdgeType] = None, **kwargs) -> SubDAG:
        node = self._some_element().child_subdag(**kwargs)

        node.add_parents(self, type=type)

        return node

    def parent_subdag(self, type: Optional[EdgeType] = None, **kwargs) -> SubDAG:
        node = self._some_element().parent_subdag(**kwargs)

        node.add_children(self, type=type)

        return node

    def add_children(self, *nodes, type: Optional[EdgeType] = None):
        for s in self:
            s.add_children(nodes, type=type)

    def remove_children(self, *nodes):
        for s in self:
            s.remove_children(nodes)

    def add_parents(self, *nodes, type: Optional[EdgeType] = None):
        for s in self:
            s.add_parents(nodes, type=type)

    def remove_parents(self, *nodes):
        for s in self:
            s.remove_parents(nodes)
