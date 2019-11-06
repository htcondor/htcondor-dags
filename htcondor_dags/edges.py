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

from typing import Tuple, Iterable, Union

import abc
import itertools

from . import node, utils, exceptions


class Join:
    def __init__(self, id):
        self.id = id


class JoinFactory:
    def __init__(self):
        self.id_generator = itertools.count(0)
        self.joins = []

    def get_join_node(self):
        j = Join(next(self.id_generator))
        self.joins.append(j)
        return j


class BaseEdge(abc.ABC):
    @abc.abstractmethod
    def get_edges(
        self, parent, child, join_factory: JoinFactory
    ) -> Iterable[
        Union[
            Tuple[Tuple[int], Tuple[int]],
            Tuple[Tuple[int], Join],
            Tuple[Join, Tuple[int]],
        ]
    ]:
        raise NotImplementedError

    def __repr__(self):
        return self.__class__.__name__


class ManyToMany(BaseEdge):
    """
    This edge connects two layers "densely": every node in the child layer
    is a child of every node in the parent layer.
    """

    def get_edges(self, parent, child, join_factory: JoinFactory):
        # TODO: this implicitly assumes that anything that isn't a NodeLayer must be a SubDAG
        num_parent_vars = len(parent.vars) if isinstance(parent, node.NodeLayer) else 1
        num_child_vars = len(child.vars) if isinstance(child, node.NodeLayer) else 1

        if num_parent_vars == 1 or num_child_vars == 1:
            yield (tuple(range(num_parent_vars)), tuple(range(num_child_vars)))
        else:
            join = join_factory.get_join_node()
            yield (tuple(range(num_parent_vars)), join)
            yield (join, tuple(range(num_child_vars)))


class OneToOne(BaseEdge):
    """
    This edge connects two layers "linearly": each underlying node in the child
    layer is a child of the corresponding underlying node with the same index
    in the parent layer.
    """

    def get_edges(self, parent, child, join_factory: JoinFactory):
        if len(parent.vars) != len(child.vars):
            raise exceptions.OneToOneEdgeNeedsSameNumberOfVars(
                f"Parent layer {parent} has {len(parent.vars)} nodes, but child layer {child} has {len(child.vars)} nodes"
            )

        yield from (((i,), (i,)) for i in range(len(parent.vars)))


class Grouper(BaseEdge):
    def __init__(self, parent_group_size=1, child_group_size=1):
        self.parent_group_size = parent_group_size
        self.child_group_size = child_group_size

    def get_edges(self, parent, child, join_factory: JoinFactory):
        num_parent_vars = len(parent.vars) if isinstance(parent, node.NodeLayer) else 1
        num_child_vars = len(child.vars) if isinstance(child, node.NodeLayer) else 1

        if num_parent_vars % self.parent_group_size != 0:
            raise exceptions.IncompatibleGrouper(
                f"Cannot apply edge {self} to layer {parent} because number of vars ({len(parent.vars)}) is not evenly divisible by the parent group size ({self.parent_group_size})"
            )
        if num_child_vars % self.child_group_size != 0:
            raise exceptions.IncompatibleGrouper(
                f"Cannot apply edge {self} to layer {child} because number of vars ({len(child.vars)}) is not evenly divisible by the child group size ({self.child_group_size})"
            )
        if (num_parent_vars // self.parent_group_size) != (
            num_child_vars // self.child_group_size
        ):
            raise exceptions.IncompatibleGrouper(
                f"Cannot apply edge {self} to layers {parent} and {child} because they do not produce the same number of groups (parent groups: {len(parent.vars)} / {self.parent_group_size} = {len(parent.vars) // self.parent_group_size}, child groups: {len(child.vars)} / {self.child_group_size} = {len(child.vars) // self.child_group_size})"
            )

        for parent_group, child_group in zip(
            utils.grouper(range(num_parent_vars), self.parent_group_size),
            utils.grouper(range(num_child_vars), self.child_group_size),
        ):
            join = join_factory.get_join_node()
            yield (parent_group, join)
            yield (join, child_group)

    def __repr__(self):
        return utils.make_repr(self, ("parent_group_size", "child_group_size"))
