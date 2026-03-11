from __future__ import annotations
from enum import Enum, auto
from dataclasses import dataclass
from neo4j import Record
from neo4j.graph import Relationship
from api.read_as_table.helpers import extract_node_label
import logging
log = logging.getLogger("[Topology]")

class NodeRole(Enum):
    LEAF = auto() # No Children, effectively a sub-table for every parent node
    CHAIN = auto() # One Child, creating a sub-table for each node
    FORK = auto() # 2+ Children, creating multiple sub-tables
    ROOT = auto()

class CycleType(Enum):
    NONE = auto()
    CROSS_TYPE = auto()
    SAME_TYPE = auto()

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

@dataclass
class SameTypeLoopInfo:
    """Which relations cause the same-type loop.
    Expansion depth is NOT decided here — that's a request-level concern."""
    relations: list[AbstractRelation]

@dataclass
class TopologyTree:
    """ Result of a Graph-Topology Analysis """
    node_label: str
    roles: set[NodeRole]
    children: list[TopologyTree]
    relation_from_parent: None | AbstractRelation
    cycle_type: CycleType
    same_type_info: None | SameTypeLoopInfo = None  # only populated when SAME_TYPE

    def __post_init__(self):
        if not isinstance(self.node_label, str):
            raise ValueError(f"{self.node_label} was not a string")
        if not all(isinstance(r, NodeRole) for r in self.roles ):
            raise ValueError(f"{self.roles} was rejected bc of type")
        if not all(isinstance(c, TopologyTree) for c in self.children):
            raise ValueError(f"{self.children} rejected bc of type.")
        if not isinstance(self.cycle_type, CycleType):
            raise ValueError(f"{self.cycle_type} was invalid")
        if self.same_type_info:
            if not isinstance(self.same_type_info, SameTypeLoopInfo):
                raise ValueError(f"{self.same_type_info} was rejected bc of type")

class TopologyTranslator:
    """ Class that abstracts Neo4j Data into a Tree of Node-Types that can be
    used for building a table"""

    def __init__(self, data:list[Record], logger=log):
        self.log = logger
        self.top, self.relations = self.topology_detector(data)

    def topology_detector(self, data: list[Record]):
        """ Iterates over the data and creates abstractions with additional data of the nodes and relations. """
        if len(data) > 1000:
            self.log.warning(f"Topology Translation may be to expansive with big data-responses. {len(data)=}")

        node_types, relations = self.extract_node_types_and_relations(data)
        log.debug(f"topology detect: \n    {node_types=}\n    {relations=} ")

        # create dict of node_name:TopologyNode
        nodes = {n:TopologyNode(n) for n in node_types}

        # evaluate relations
        for r in relations:
            from_node = nodes[r.from_node_type]
            to_node = nodes[r.to_node_type]
            from_node.connected_to.append((to_node, r))
            to_node.incoming_con_n += 1

        top = sorted([n for n in nodes.values()], key=lambda node: -len(node.connected_to))
        return top, relations


    def get_topology_tree(self) -> list[TopologyTree]:
        """Returns a list of TopologyTree roots representing the full schema topology.

        - Same-type loops are captured as metadata (not recursed into)
        - Cross-type loops are detected via per-path ancestors and marked
        - Neither loop type is expanded — that's the consumer's job
        """
        if not self.top: return []

        roots = self.roots
        if not roots:
            self.log.warning("No root nodes found — possible full cycle. "
                            "Using first node as fallback.")
            roots = [self.top[0]]

        trees = []
        for root in roots:
            tree = self._build_tree(root, ancestors=set())
            trees.append(tree)

        return trees


    def _build_tree(self, node: TopologyNode, ancestors: set) -> TopologyTree:
        # Loop Detection
        same_type = [(child, rel) for child, rel in node.connected_to
                    if child.node_lbl == node.node_lbl]
        cross_type = [(child, rel) for child, rel in node.connected_to
                    if child.node_lbl != node.node_lbl]

        same_type_info = None
        if same_type:
            same_type_info = SameTypeLoopInfo(
                relations=[rel for _, rel in same_type]
            )

        # Cross-type loop Detection
        if node.node_lbl in ancestors:
            return TopologyTree(
                node_label=node.node_lbl,
                roles=node.get_classification(),
                children=[],               # hard stop — don't recurse
                relation_from_parent=None,  # caller sets this after return
                cycle_type=CycleType.CROSS_TYPE,
                same_type_info=same_type_info  # could still have same-type loops
            )

        # --- Normal node: recurse into cross-type children only ---
        ancestors.add(node.node_lbl)

        children = []
        for child, relation in cross_type:
            child_tree = self._build_tree(child, ancestors)
            child_tree.relation_from_parent = relation
            children.append(child_tree)

        ancestors.remove(node.node_lbl)

        return TopologyTree(
            node_label=node.node_lbl,
            roles=node.get_classification(),
            children=children,
            relation_from_parent=None,
            cycle_type=CycleType.SAME_TYPE if same_type else CycleType.NONE,
            same_type_info=same_type_info
        )


    def extract_node_types_and_relations(self, data: list[Record])-> tuple[set[str], set[AbstractRelation]]:
        """ iterates over all records, finding node types and relations
        this may become to expansive at huge datasets."""
        known_nodes= set()
        relations = set()
        for record in data:
            for element in record:

                if isinstance(element, Relationship):

                    n1 = element.nodes[0]
                    n2 = element.nodes[1]
                    if not n1 or not n2:
                        self.log.warning("Found a None-Node in relations.")
                        continue

                    l1 = extract_node_label(n1)
                    l2 = extract_node_label(n2)
                    label = element.type

                    r = AbstractRelation(label, l1, l2)
                    if r in relations: continue
                    relations.add(r)
                    continue

                else:
                    label = extract_node_label(element)

                    if label in known_nodes: continue
                    known_nodes.add(label)


        self.log.info(f"Extracted {known_nodes} and {relations}")

        return known_nodes, relations

    @property
    def roots(self):
        return [node for node in self.top if node.is_root]

    def print_topology(self):
        """
        Development / Debugging Method for Visualization
        """
        if not self.top: return None
        roots = self.roots
        if not roots: roots = [self.top[0]]
        nr_nodes = len(self.top)
        longest_path = max(get_longest_path(r) for r in roots)
        send_info(f"Found roots: {roots},  {longest_path=}  {nr_nodes=}")

        for root in roots:
            send_info(f"\n ----Iterative Tree Crawling----")
            self._iter_printer(root)
            send_info(f"\n----recursive Tree Crawling with max_depth----")
            self._rec_tree_eval(root, max_depth=(max(nr_nodes, longest_path)))

    def _iter_printer(self, root):
        """
        Development / Debugging Method for Visualization.
        Used by print_topology.
        """
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
        """
        Development / Debugging Method for Visualization.
        Used by print_topology.
        """
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




def send_info(msg:str):
    log.debug(msg)

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






