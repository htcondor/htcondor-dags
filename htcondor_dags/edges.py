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

from . import node, exceptions


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
            return [(tuple(range(num_parent_vars)), tuple(range(num_child_vars)))]
        else:
            join = join_factory.get_join_node()
            parent_to_join = (tuple(range(num_parent_vars)), join)
            join_to_child = (join, tuple(range(num_child_vars)))
            return parent_to_join, join_to_child


class OneToOne(BaseEdge):
    """
    This edge connects two layers "linearly": each underlying node in the child
    layer is a child of the corresponding underlying node with the same index
    in the parent layer.
    """

    def get_edges(self, parent, child, join_factory: JoinFactory):
        if len(parent.vars) != len(child.vars):
            raise exceptions.OneToOneEdgeNeedsSameNumberOfVars(
                f"parent layer {parent} has {len(parent.vars)} nodes, but child layer {child} has {len(child.vars)} nodes"
            )

        return (((i,), (i,)) for i in range(len(parent.vars)))
