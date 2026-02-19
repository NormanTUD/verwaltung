from enum import Enum, auto
from dataclasses import dataclass
from neo4j import Record
from neo4j.graph import Relationship
import logging
log = logging.getLogger("[Topology]")

class NodeRole(Enum):
    LEAF = auto() # No Children, effectively a sub-table for every parent node
    CHAIN = auto() # One Child, creating a sub-table for each node
    FORK = auto() # 2+ Children, creating multiple sub-tables
    ROOT = auto()

class TopologyNode:
    def __init__(self, node_lbl:str):
        self.node_lbl = node_lbl
        self.connected_to:list[tuple[TopologyNode, AbstractRelation]] = []
        self.incoming_con_n = 0

    @property
    def is_root(self) -> bool:
        if self.incoming_con_n == 0: return True
        self_refs = [n[0] for n in self.connected_to].count(self)
        return self_refs == self.incoming_con_n

    @property
    def is_leaf(self) -> bool:
        return len(self.connected_to) == 0

    def get_classification(self):
        roles = set()
        if self.is_root: roles.add(NodeRole.ROOT)
        if self.is_leaf: roles.add(NodeRole.LEAF)
        if len(self.connected_to) > 1: roles.add(NodeRole.FORK)
        if len(self.connected_to) == 1: roles.add(NodeRole.CHAIN)
        return roles

    def __repr__(self):
        return f"{self.node_lbl}: of type {self.get_classification()} with {len(self.connected_to)} children and {self.incoming_con_n} parents. "


@dataclass(frozen=True)
class AbstractRelation:
    label :str
    from_node_type :str
    to_node_type :str


class TopologyTranslator:
    def __init__(self, data:list[Record], logger=log):
        self.log = log
        self.top, self.relations = self.topology_detector(data)
    """ Class that transforms Neo4j Data into a json-readable table"""

    def topology_detector(self, data: list[Record]):
        node_types, relations = self.extract_node_types_and_relations(data)
        log.info(f"topology detect: \n    {node_types=}\n    {relations=} ")
        nodes = {n:TopologyNode(n) for n in node_types}

        for r in relations:
            # Wont work for relation between two nodes of same type
            # wed create a TopNode that points to itself, is no root
            from_node = nodes[r.from_node_type]
            to_node = nodes[r.to_node_type]
            from_node.connected_to.append((to_node, r))
            to_node.incoming_con_n += 1

        top = top = sorted([n for n in nodes.values()], key=lambda node: -len(node.connected_to))
        return top, relations


    def extract_node_types_and_relations(self, data: list[Record])-> tuple[set[str], set[AbstractRelation]]:
        known_nodes= set()
        relations = set()
        for record in data:
            for element in record:

                if isinstance(element, Relationship):
                    n1 = element.nodes[0]
                    l1, = n1.labels
                    n2 = element.nodes[1]
                    l2, = n2.labels
                    label = element.type

                    r = AbstractRelation(label, l1, l2)
                    if r in relations: continue
                    relations.add(r)
                    continue

                label = list(element.labels)[0]
                if label in known_nodes: continue
                known_nodes.add(label)

        self.log.info(f"Extracted {known_nodes} and {relations}")

        return known_nodes, relations

    def print_topology(self):
        if not self.top: return None
        roots = [node for node in self.top if node.is_root]
        if not roots: roots = [self.top[0]]
        nr_nodes = len(self.top)
        longest_path = get_longest_path(self.top[0])
        send_info(f"Found roots: {roots},  {longest_path=}  {nr_nodes=}")

        for root in roots:
            send_info(f"\n ----Iterative Tree Crawling----")
            self._iter_printer(root)
            send_info(f"\n----recursive Tree Crawling with max_depth----")
            self._rec_tree_eval(root, max_depth=(max(nr_nodes, longest_path)))


    def _iter_printer(self, root):
        visited = set()
        indent = "    "
        frontier = [(root, 0)] # int keeps track of indentation
        send_info(f"Graph of {root}")
        while frontier:
            current_node, depth = frontier.pop(0)
            if current_node in visited:
                continue
            visited.add(current_node)
            send_info(f"{indent*depth}{current_node}")

            # print connections
            connection = current_node.connected_to
            if connection:
                for child, relation in connection:
                    send_info(f"{indent * (depth+1)}-->{relation}->{child}")
                    if not child in visited: frontier.append((child, depth+1))

    def _rec_tree_eval(self, node, max_depth, depth=0, ancestors=None):
        if ancestors is None:
            ancestors = set()

        if node.node_lbl in ancestors:
            indent = "    " * depth
            send_info(f"{indent}↩ [{node.node_lbl}] (CROSS_TYPE_LOOP — already in this path)")
            return

        if depth >= max_depth:
            return

        ancestors.add(node.node_lbl)
        indent = "    " * depth

        # Separate same-type loops from cross-type children
        same_type = [(c, r) for c, r in node.connected_to if c.node_lbl == node.node_lbl]
        cross_type = [(c, r) for c, r in node.connected_to if c.node_lbl != node.node_lbl]

        if same_type:
            rel_names = [r.label for _, r in same_type]
            send_info(f"{indent}[{node.node_lbl}] {node.get_classification()} "
                    f"⟳ SAME_TYPE_LOOP via {rel_names}")
        else:
            send_info(f"{indent}[{node.node_lbl}] {node.get_classification()}")

        for child, relation in cross_type:
            send_info(f"{indent}    --[{relation.label}]-->")
            self._rec_tree_eval(child, max_depth, depth + 1, ancestors)

        ancestors.remove(node.node_lbl)


def do_something(node):
    print(node)

def send_info(msg:str):
    print(msg)

def get_longest_path(node:TopologyNode, visited:set|None=None):
    """ Recursive traversel of a tree to find the longest path. """

    if visited is None: visited = set()
    if node.is_leaf: return 1
    if node in visited: return 0
    visited.add(node)
    children = [c[0] for c in node.connected_to]
    result = max(get_longest_path(child, visited) for child in children) +1
    visited.remove(node)
    return result






