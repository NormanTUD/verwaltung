"""
pytest suite for api.read_as_table.topology_detector

Two construction paths are tested:
  1. FakeRecord → TopologyTranslator.__init__   (integration — exercises record parsing)
  2. build_translator() from topology_cli.py     (unit — bypasses Neo4j records)

Coverage map (maps to topology_cli_run_tests.py catalogue):
  DOC-01  chain           TestSimpleChain
  DOC-02  fork            TestSimpleFork
  DOC-03  two rels        TestMultipleRelationsSamePair
  DOC-04  bidirectional   TestCrossTypeCycle.test_bidirectional_pair
  DOC-05  diamond         TestDiamondIntegration + TestDiamondVariants
  DOC-06  diamond+ext     TestDiamondVariants.test_diamond_with_extension
  DOC-07  triangle cycle  TestCrossTypeCycle.test_triangle_cycle_detected
  DOC-08  4-node cycle    TestCrossTypeCycle.test_four_node_cycle
  DOC-09  self-loop       TestSameTypeLoop.test_single_self_relation
  DOC-10  two self-loops  TestSameTypeLoop.test_two_self_relations
  EDGE-01 isolated node   TestIsolatedNode
  EDGE-02 two isolated    TestIsolatedNode.test_two_isolated_nodes
  EDGE-03 disconnected    TestDisconnectedSubgraphs
  EDGE-04 no natural root TestCrossTypeCycle.test_full_cycle_uses_fallback_root
  EDGE-05 diamond false+  TestDiamondVariants.test_diamond_convergence_not_cross_type
  EDGE-06 mixed loop      TestSameTypeLoop.test_mixed_same_and_cross_type
  EDGE-07 deep chain      TestDeepAndWide.test_deep_chain_7_levels
  EDGE-08 wide fork       TestDeepAndWide.test_wide_fork_6_children
  EDGE-09 diamond+self    TestDiamondVariants.test_diamond_plus_self_loop
  EDGE-10 subtree cycle   TestCrossTypeCycle.test_cycle_hanging_off_tree
  EDGE-11 multi-root      TestMultiRoot
  EDGE-12 self-loop root  TestSelfLoopRootDetection
  EDGE-13 bidir+ext       TestBidirectionalExtension
  EDGE-14 realistic       TestRealisticSchema

Run:  pytest test_topology.py -v
"""
from __future__ import annotations

import pytest
from neo4j.graph import Relationship

from api.read_as_table.topology_detector import (
    AbstractRelation,
    CycleType,
    NodeRole,
    SameTypeLoopInfo,
    TopologyNode,
    TopologyTranslator,
    TopologyTree,
    _expand_same_type,
    get_longest_path,
)
from api.read_as_table.topology_cli import build_translator


# ════════════════════════════════════════════════
#  Fakes for integration tests (mimic neo4j types)
# ════════════════════════════════════════════════

class FakeNode:
    """Mimics neo4j.graph.Node with just the .labels interface."""
    def __init__(self, label: str):
        self.labels = frozenset([label])


class FakeRelationship(Relationship):
    """Subclass of neo4j.graph.Relationship so isinstance() checks pass.
    Overrides __init__ to avoid needing a real Graph object."""
    def __init__(self, start_node: FakeNode, end_node: FakeNode, rel_type: str = "RELATED_TO"):
        self._nodes = (start_node, end_node)
        self._rel_type = rel_type

    @property
    def nodes(self):
        return self._nodes

    @property
    def type(self) -> str:
        return self._rel_type


class FakeRecord:
    """Mimics neo4j.Record — iterable over its elements."""
    def __init__(self, *elements):
        self._elements = list(elements)

    def __iter__(self):
        return iter(self._elements)


# ════════════════════════════════════════════════
#  Helpers
# ════════════════════════════════════════════════

def _labels(trees: list[TopologyTree]) -> set[str]:
    """Collect every node_label reachable from the given roots."""
    out: set[str] = set()
    def _walk(t: TopologyTree):
        out.add(t.node_label)
        for c in t.children:
            _walk(c)
    for tree in trees:
        _walk(tree)
    return out


def _find_all(trees: list[TopologyTree], label: str) -> list[TopologyTree]:
    """Return every TopologyTree node whose node_label matches."""
    found: list[TopologyTree] = []
    def _walk(t: TopologyTree):
        if t.node_label == label:
            found.append(t)
        for c in t.children:
            _walk(c)
    for tree in trees:
        _walk(tree)
    return found


def _root_labels(trees: list[TopologyTree]) -> set[str]:
    return {t.node_label for t in trees}


def _leaf_labels(trees: list[TopologyTree]) -> set[str]:
    """Labels that appear with no children in the tree."""
    out: set[str] = set()
    def _walk(t: TopologyTree):
        if not t.children:
            out.add(t.node_label)
        for c in t.children:
            _walk(c)
    for tree in trees:
        _walk(tree)
    return out


def _make_translator(nodes_csv: str, rel_strings: list[str]):
    """Shortcut: build a TopologyTranslator from CSV + relation strings."""
    node_labels = {n.strip() for n in nodes_csv.split(",") if n.strip()}
    relations: set[AbstractRelation] = set()
    for r in rel_strings:
        parts = r.strip().split()
        assert len(parts) == 3, f"Bad relation format: '{r}'"
        relations.add(AbstractRelation(label=parts[1],
                                       from_node_type=parts[0],
                                       to_node_type=parts[2]))
    return build_translator(node_labels, relations)


# ════════════════════════════════════════════════
#  Fixtures
# ════════════════════════════════════════════════

@pytest.fixture
def diamond_records():
    """FakeRecord data for:
        root ──has──→ movie ──written_by──→ person
          └───has──→ book  ──written_by──┘
    """
    root = FakeNode("root")
    person = FakeNode("person")
    movie = FakeNode("movie")
    book = FakeNode("book")
    return [
        FakeRecord(root, FakeRelationship(root, movie, "has"), movie),
        FakeRecord(root, FakeRelationship(root, book, "has"), book),
        FakeRecord(movie, FakeRelationship(movie, person, "written_by"), person),
        FakeRecord(book, FakeRelationship(book, person, "written_by"), person),
    ]


@pytest.fixture
def chain_abc():
    """A → B → C  (simple chain)."""
    return _make_translator("A,B,C", ["A R1 B", "B R2 C"])


@pytest.fixture
def fork_person():
    """Person → Company, Person → City  (simple fork)."""
    return _make_translator("Person,Company,City",
                            ["Person WORKS_AT Company",
                             "Person LIVES_IN City"])


@pytest.fixture
def diamond():
    """A → B, A → C, B → D, C → D  (abstract diamond)."""
    return _make_translator("A,B,C,D",
                            ["A R1 B", "A R2 C", "B R3 D", "C R4 D"])


@pytest.fixture
def self_loop_employee():
    """Employee MANAGES Employee (single same-type loop)."""
    return _make_translator("Employee", ["Employee MANAGES Employee"])


@pytest.fixture
def cross_type_triangle():
    """A → B → C → A  (cross-type cycle)."""
    return _make_translator("A,B,C", ["A R1 B", "B R2 C", "C R3 A"])


# ════════════════════════════════════════════════
#  1. TopologyNode unit tests
# ════════════════════════════════════════════════

class TestTopologyNode:

    def test_fresh_node_is_root_and_leaf(self):
        node = TopologyNode("X")
        assert node.is_root is True
        assert node.is_leaf is True
        assert node.get_classification() == {NodeRole.ROOT, NodeRole.LEAF}

    def test_node_with_children_is_not_leaf(self):
        parent = TopologyNode("P")
        child = TopologyNode("C")
        parent.connected_to.append((child, AbstractRelation("R", "P", "C")))
        assert parent.is_leaf is False

    def test_node_with_incoming_is_not_root(self):
        node = TopologyNode("N")
        node.incoming_con_n = 1
        assert node.is_root is False

    def test_chain_classification(self):
        node = TopologyNode("N")
        node.connected_to.append(
            (TopologyNode("C"), AbstractRelation("R", "N", "C")))
        roles = node.get_classification()
        assert NodeRole.CHAIN in roles
        assert NodeRole.FORK not in roles

    def test_fork_classification(self):
        node = TopologyNode("N")
        for i in range(3):
            node.connected_to.append(
                (TopologyNode(f"C{i}"), AbstractRelation(f"R{i}", "N", f"C{i}")))
        roles = node.get_classification()
        assert NodeRole.FORK in roles
        assert NodeRole.CHAIN not in roles

    def test_self_ref_node_is_still_root(self):
        """is_root checks self_refs == incoming_con_n."""
        node = TopologyNode("A")
        node.connected_to.append(
            (node, AbstractRelation("SELF", "A", "A")))
        node.incoming_con_n = 1
        assert node.is_root is True


# ════════════════════════════════════════════════
#  2. TopologyTree __post_init__ validation
# ════════════════════════════════════════════════

class TestTopologyTreeValidation:

    def test_valid_tree_creates_successfully(self):
        tree = TopologyTree(
            node_label="X", roles={NodeRole.ROOT, NodeRole.LEAF},
            children=[], relation_from_parent=None, cycle_type=CycleType.NONE,
        )
        assert tree.node_label == "X"

    def test_rejects_non_string_label(self):
        with pytest.raises(ValueError, match="was not a string"):
            TopologyTree(node_label=42, roles={NodeRole.ROOT}, children=[],
                         relation_from_parent=None, cycle_type=CycleType.NONE)

    def test_rejects_invalid_role_type(self):
        with pytest.raises(ValueError, match="was rejected bc of type"):
            TopologyTree(node_label="X", roles={"NOT_A_ROLE"}, children=[],
                         relation_from_parent=None, cycle_type=CycleType.NONE)

    def test_rejects_invalid_child_type(self):
        with pytest.raises(ValueError, match="rejected bc of type"):
            TopologyTree(node_label="X", roles={NodeRole.ROOT},
                         children=["not_a_tree"],
                         relation_from_parent=None, cycle_type=CycleType.NONE)

    def test_rejects_invalid_cycle_type(self):
        with pytest.raises(ValueError, match="was invalid"):
            TopologyTree(node_label="X", roles={NodeRole.ROOT}, children=[],
                         relation_from_parent=None, cycle_type="NONE")

    def test_rejects_invalid_same_type_info(self):
        with pytest.raises(ValueError, match="was rejected bc of type"):
            TopologyTree(node_label="X", roles={NodeRole.ROOT}, children=[],
                         relation_from_parent=None, cycle_type=CycleType.SAME_TYPE,
                         same_type_info="bad")


# ════════════════════════════════════════════════
#  3. Integration: Diamond via FakeRecords
#
#     THE ONLY TESTS that exercise the full pipeline:
#     FakeRecord → __init__ → extract_node_types_and_relations
#     → topology_detector → get_topology_tree → _build_tree
# ════════════════════════════════════════════════

class TestDiamondIntegration:
    """root ──has──→ movie ──written_by──→ person
       root ──has──→ book  ──written_by──→ person"""

    def test_record_parsing(self, diamond_records):
        """extract_node_types_and_relations correctly parses FakeRecords."""
        tt = TopologyTranslator(diamond_records)
        assert len(tt.top) == 4  # root, person, movie, book
        roots = tt.roots
        assert len(roots) == 1
        assert roots[0].node_lbl == "root"

    def test_full_tree_structure(self, diamond_records):
        """Comprehensive structural verification: root, middle layer,
        diamond convergence at leaf, roles, and relations."""
        tt = TopologyTranslator(diamond_records)
        trees = tt.get_topology_tree()
        assert len(trees) == 1
        tree = trees[0]

        # Root
        assert tree.node_label == "root"
        assert NodeRole.ROOT in tree.roles
        assert NodeRole.FORK in tree.roles
        assert tree.relation_from_parent is None
        assert len(tree.children) == 2

        # Middle layer (movie, book)
        by_label = {c.node_label: c for c in tree.children}
        assert set(by_label.keys()) == {"movie", "book"}
        for mid in tree.children:
            assert NodeRole.CHAIN in mid.roles
            assert mid.relation_from_parent.label == "has"
            assert mid.relation_from_parent.from_node_type == "root"
            assert len(mid.children) == 1

        # Leaf layer — THE diamond property: person under BOTH branches
        movie_person = by_label["movie"].children[0]
        book_person = by_label["book"].children[0]
        assert movie_person.node_label == "person"
        assert book_person.node_label == "person"
        assert movie_person is not book_person  # separate TopologyTree instances
        for p in (movie_person, book_person):
            assert NodeRole.LEAF in p.roles
            assert p.children == []
            assert p.relation_from_parent.label == "written_by"
            assert p.relation_from_parent.to_node_type == "person"

    def test_no_false_cycle_detection(self, diamond_records):
        """Diamond convergence is NOT a cycle — no node should be CROSS/SAME_TYPE."""
        tt = TopologyTranslator(diamond_records)
        tree = tt.get_topology_tree()[0]

        def assert_no_cycles(node: TopologyTree):
            assert node.cycle_type == CycleType.NONE, \
                f"{node.node_label} incorrectly marked {node.cycle_type}"
            assert node.same_type_info is None
            for child in node.children:
                assert_no_cycles(child)

        assert_no_cycles(tree)


# ════════════════════════════════════════════════
#  4. Simple chain (A → B → C)
# ════════════════════════════════════════════════

class TestSimpleChain:
    """DOC-01: Linear chain."""

    def test_root_and_roles(self, chain_abc):
        trees = chain_abc.get_topology_tree()
        assert len(trees) == 1
        assert trees[0].node_label == "A"
        assert NodeRole.ROOT in trees[0].roles

    def test_labels_and_leaf(self, chain_abc):
        trees = chain_abc.get_topology_tree()
        assert _labels(trees) == {"A", "B", "C"}
        assert _leaf_labels(trees) == {"C"}

    def test_depth_structure(self, chain_abc):
        trees = chain_abc.get_topology_tree()
        root = trees[0]
        assert len(root.children) == 1
        mid = root.children[0]
        assert mid.node_label == "B"
        assert len(mid.children) == 1
        leaf = mid.children[0]
        assert leaf.node_label == "C"
        assert len(leaf.children) == 0

    def test_no_cycles_and_relations(self, chain_abc):
        trees = chain_abc.get_topology_tree()
        for n in _find_all(trees, "A") + _find_all(trees, "B") + _find_all(trees, "C"):
            assert n.cycle_type == CycleType.NONE
        assert trees[0].relation_from_parent is None
        b_node = trees[0].children[0]
        assert b_node.relation_from_parent is not None
        assert b_node.relation_from_parent.label == "R1"


# ════════════════════════════════════════════════
#  5. Simple fork (Person → Company, City)
# ════════════════════════════════════════════════

class TestSimpleFork:
    """DOC-02: Fork topology."""

    def test_root_with_fork_role(self, fork_person):
        trees = fork_person.get_topology_tree()
        assert len(trees) == 1
        assert trees[0].node_label == "Person"
        assert NodeRole.FORK in trees[0].roles

    def test_fork_children(self, fork_person):
        trees = fork_person.get_topology_tree()
        child_labels = {c.node_label for c in trees[0].children}
        assert child_labels == {"Company", "City"}
        for child in trees[0].children:
            assert len(child.children) == 0


# ════════════════════════════════════════════════
#  6. Isolated and disconnected nodes
# ════════════════════════════════════════════════

class TestIsolatedNode:
    """EDGE-01/02: Nodes with no relations."""

    def test_single_isolated_node(self):
        t = _make_translator("Orphan", [])
        trees = t.get_topology_tree()
        assert len(trees) == 1
        assert trees[0].node_label == "Orphan"
        assert NodeRole.ROOT in trees[0].roles
        assert NodeRole.LEAF in trees[0].roles
        assert trees[0].children == []

    def test_two_isolated_nodes(self):
        t = _make_translator("Alpha,Beta", [])
        trees = t.get_topology_tree()
        assert len(trees) == 2
        assert _root_labels(trees) == {"Alpha", "Beta"}
        for tree in trees:
            assert NodeRole.ROOT in tree.roles
            assert NodeRole.LEAF in tree.roles


class TestDisconnectedSubgraphs:
    """EDGE-03: A → B and C → D — two separate components."""

    def test_two_roots_all_nodes_covered(self):
        t = _make_translator("A,B,C,D", ["A R1 B", "C R2 D"])
        trees = t.get_topology_tree()
        assert len(trees) == 2
        assert _root_labels(trees) == {"A", "C"}
        assert _labels(trees) == {"A", "B", "C", "D"}


# ════════════════════════════════════════════════
#  7. Same-type loops (Type C)
# ════════════════════════════════════════════════

class TestSameTypeLoop:

    def test_single_self_relation(self, self_loop_employee):
        """DOC-09: Employee MANAGES Employee — detection + metadata."""
        trees = self_loop_employee.get_topology_tree()
        assert len(trees) == 1
        root = trees[0]
        assert root.node_label == "Employee"
        assert root.cycle_type == CycleType.SAME_TYPE
        assert root.same_type_info is not None
        assert len(root.same_type_info.relations) == 1
        assert root.same_type_info.relations[0].label == "MANAGES"

    def test_two_self_relations(self):
        """DOC-10: Employee MANAGES + MENTORS Employee."""
        t = _make_translator("Employee",
                             ["Employee MANAGES Employee",
                              "Employee MENTORS Employee"])
        trees = t.get_topology_tree()
        root = trees[0]
        assert root.cycle_type == CycleType.SAME_TYPE
        rel_labels = {r.label for r in root.same_type_info.relations}
        assert rel_labels == {"MANAGES", "MENTORS"}

    def test_mixed_same_and_cross_type(self):
        """EDGE-06: Manager MANAGES Manager + Manager OVERSEES Project."""
        t = _make_translator("Manager,Project",
                             ["Manager MANAGES Manager",
                              "Manager OVERSEES Project"])
        trees = t.get_topology_tree()
        root = trees[0]
        assert root.node_label == "Manager"
        assert root.cycle_type == CycleType.SAME_TYPE
        assert root.same_type_info is not None
        cross_children = [c for c in root.children if c.node_label == "Project"]
        assert len(cross_children) >= 1


# ════════════════════════════════════════════════
#  8. Cross-type cycles
# ════════════════════════════════════════════════

class TestCrossTypeCycle:

    def test_triangle_cycle_detected(self, cross_type_triangle):
        """DOC-07: A → B → C → A — at least one CROSS_TYPE node."""
        trees = cross_type_triangle.get_topology_tree()
        assert len(trees) >= 1
        all_nodes = _find_all(trees, "A") + _find_all(trees, "B") + _find_all(trees, "C")
        cross_type_nodes = [n for n in all_nodes if n.cycle_type == CycleType.CROSS_TYPE]
        assert len(cross_type_nodes) >= 1, "Expected at least one CROSS_TYPE marker"

    def test_cross_type_node_has_no_children(self, cross_type_triangle):
        """Cross-type loop detection should hard-stop recursion."""
        trees = cross_type_triangle.get_topology_tree()
        all_nodes = _find_all(trees, "A") + _find_all(trees, "B") + _find_all(trees, "C")
        for node in all_nodes:
            if node.cycle_type == CycleType.CROSS_TYPE:
                assert node.children == [], \
                    f"CROSS_TYPE node {node.node_label} should have no children"

    def test_four_node_cycle(self):
        """DOC-08: A → B → C → D → A."""
        t = _make_translator("A,B,C,D",
                             ["A R1 B", "B R2 C", "C R3 D", "D R4 A"])
        trees = t.get_topology_tree()
        assert len(trees) >= 1
        assert _labels(trees) == {"A", "B", "C", "D"}

    def test_full_cycle_uses_fallback_root(self):
        """EDGE-04: When no node qualifies as root, fallback should be used."""
        t = _make_translator("A,B,C", ["A R1 B", "B R2 C", "C R3 A"])
        assert t.roots == []
        trees = t.get_topology_tree()
        assert len(trees) >= 1

    def test_cycle_hanging_off_tree(self):
        """EDGE-10: Root → X → Y → Z → X. Root is clean, subtree has cycle."""
        t = _make_translator("Root,X,Y,Z",
                             ["Root R1 X", "X R2 Y", "Y R3 Z", "Z R4 X"])
        trees = t.get_topology_tree()
        assert len(trees) == 1
        assert trees[0].node_label == "Root"
        x_nodes = _find_all(trees, "X")
        cross_x = [n for n in x_nodes if n.cycle_type == CycleType.CROSS_TYPE]
        assert len(cross_x) >= 1, "X should be flagged CROSS_TYPE when revisited"

    def test_bidirectional_pair(self):
        """DOC-04: Person WORKS_AT Company AND Company EMPLOYS Person."""
        t = _make_translator("Person,Company",
                             ["Person WORKS_AT Company",
                              "Company EMPLOYS Person"])
        trees = t.get_topology_tree()
        assert len(trees) >= 1
        all_nodes = _find_all(trees, "Person") + _find_all(trees, "Company")
        cross_nodes = [n for n in all_nodes if n.cycle_type == CycleType.CROSS_TYPE]
        assert len(cross_nodes) >= 1


# ════════════════════════════════════════════════
#  9. Diamond variants (build_translator path)
# ════════════════════════════════════════════════

class TestDiamondVariants:
    """DOC-05/06, EDGE-05/09: Diamond problem and its extensions.
    Core diamond integration is covered by TestDiamondIntegration (FakeRecords).
    These tests use build_translator for abstract topology variants."""

    def test_diamond_convergence_not_cross_type(self, diamond):
        """EDGE-05: D should NOT be flagged as CROSS_TYPE — it's reachable
        via two separate paths but is not an ancestor of itself."""
        trees = diamond.get_topology_tree()
        assert _labels(trees) == {"A", "B", "C", "D"}
        assert len(trees) == 1
        assert trees[0].node_label == "A"
        d_nodes = _find_all(trees, "D")
        assert len(d_nodes) >= 2, (
            f"Expected D in both branches but found {len(d_nodes)} occurrence(s)."
        )
        for d in d_nodes:
            assert d.cycle_type != CycleType.CROSS_TYPE, \
                "D incorrectly marked as CROSS_TYPE — diamond false positive"

    def test_diamond_with_extension(self):
        """DOC-06: A → B, A → C, B → D, C → D, D → E."""
        t = _make_translator("A,B,C,D,E",
                             ["A R1 B", "A R2 C", "B R3 D",
                              "C R4 D", "D R5 E"])
        trees = t.get_topology_tree()
        assert _labels(trees) == {"A", "B", "C", "D", "E"}
        e_nodes = _find_all(trees, "E")
        assert len(e_nodes) >= 1

    def test_diamond_plus_self_loop(self):
        """EDGE-09: Diamond + D LINKS D."""
        t = _make_translator("A,B,C,D",
                             ["A R1 B", "A R2 C", "B R3 D",
                              "C R4 D", "D LINKS D"])
        trees = t.get_topology_tree()
        d_nodes = _find_all(trees, "D")
        d_with_loop = [d for d in d_nodes if d.same_type_info is not None]
        assert len(d_with_loop) >= 1, "D's self-loop should be detected"


# ════════════════════════════════════════════════
#  10. _expand_same_type
# ════════════════════════════════════════════════

class TestExpandSameType:
    """Tests for the _expand_same_type function.

    Known bugs (see code review):
      BUG 1: `return` should be `continue` — causes early exit on revisited nodes
      BUG 2: `if i == depth` inside `range(depth)` is never true — LEAF never assigned
    """

    def _make_same_type_tree(self) -> TopologyTree:
        """Helper: a simple SAME_TYPE root node."""
        return TopologyTree(
            node_label="Employee",
            roles={NodeRole.ROOT},
            children=[],
            relation_from_parent=None,
            cycle_type=CycleType.SAME_TYPE,
            same_type_info=SameTypeLoopInfo(
                relations=[AbstractRelation("MANAGES", "Employee", "Employee")]
            ),
        )

    def test_expand_adds_children(self):
        """Expansion with depth=1 should add at least one child clone."""
        tree = self._make_same_type_tree()
        added = _expand_same_type(tree, depth=1)
        assert added is not None
        assert len(added) >= 1
        assert len(tree.children) >= 1
        assert tree.children[0].node_label == "Employee"

    def test_expand_removes_root_from_child(self):
        """Cloned children should not have ROOT role."""
        tree = self._make_same_type_tree()
        _expand_same_type(tree, depth=1)
        for child in tree.children:
            assert NodeRole.ROOT not in child.roles

    def test_expand_removes_leaf_from_parent(self):
        """Parent should lose LEAF role when expanded."""
        tree = TopologyTree(
            node_label="Employee",
            roles={NodeRole.ROOT, NodeRole.LEAF},
            children=[],
            relation_from_parent=None,
            cycle_type=CycleType.SAME_TYPE,
            same_type_info=SameTypeLoopInfo(
                relations=[AbstractRelation("MANAGES", "Employee", "Employee")]
            ),
        )
        _expand_same_type(tree, depth=1)
        assert NodeRole.LEAF not in tree.roles

    def test_expand_deepest_clone_is_leaf(self):
        """The deepest expanded clone should be marked LEAF."""
        tree = self._make_same_type_tree()
        added = _expand_same_type(tree, depth=2)
        assert added is not None and len(added) == 2
        deepest = added[-1]
        assert NodeRole.LEAF in deepest.roles

    def test_expand_does_not_exit_early_on_visited_sibling(self):
        """If a parent has two same-type children, both should be expanded."""
        child_a = TopologyTree(
            node_label="Worker", roles={NodeRole.LEAF}, children=[],
            relation_from_parent=AbstractRelation("R1", "Boss", "Worker"),
            cycle_type=CycleType.SAME_TYPE,
            same_type_info=SameTypeLoopInfo(
                relations=[AbstractRelation("ASSISTS", "Worker", "Worker")]
            ),
        )
        child_b = TopologyTree(
            node_label="Worker", roles={NodeRole.LEAF}, children=[],
            relation_from_parent=AbstractRelation("R2", "Boss", "Worker"),
            cycle_type=CycleType.SAME_TYPE,
            same_type_info=SameTypeLoopInfo(
                relations=[AbstractRelation("ASSISTS", "Worker", "Worker")]
            ),
        )
        root = TopologyTree(
            node_label="Boss", roles={NodeRole.ROOT, NodeRole.FORK},
            children=[child_a, child_b],
            relation_from_parent=None, cycle_type=CycleType.NONE,
        )
        _expand_same_type(root, depth=1)
        assert len(child_a.children) >= 1, "child_a was not expanded"
        assert len(child_b.children) >= 1, "child_b was not expanded (early exit bug)"

    def test_expand_depth_zero_is_noop(self):
        """depth=0 should not add any children."""
        tree = self._make_same_type_tree()
        original_children = len(tree.children)
        added = _expand_same_type(tree, depth=0)
        assert len(tree.children) == original_children
        assert added == []


# ════════════════════════════════════════════════
#  11. get_longest_path
# ════════════════════════════════════════════════

class TestGetLongestPath:

    def test_single_node(self):
        node = TopologyNode("A")
        assert get_longest_path(node) == 1

    def test_chain_of_three(self):
        a = TopologyNode("A")
        b = TopologyNode("B")
        c = TopologyNode("C")
        a.connected_to.append((b, AbstractRelation("R1", "A", "B")))
        b.connected_to.append((c, AbstractRelation("R2", "B", "C")))
        assert get_longest_path(a) == 3

    def test_fork_returns_longest_branch(self):
        root = TopologyNode("Root")
        short = TopologyNode("Short")
        long1 = TopologyNode("Long1")
        long2 = TopologyNode("Long2")
        root.connected_to.append((short, AbstractRelation("R1", "Root", "Short")))
        root.connected_to.append((long1, AbstractRelation("R2", "Root", "Long1")))
        long1.connected_to.append((long2, AbstractRelation("R3", "Long1", "Long2")))
        assert get_longest_path(root) == 3

    def test_self_loop_does_not_infinite_recurse(self):
        node = TopologyNode("A")
        node.connected_to.append((node, AbstractRelation("SELF", "A", "A")))
        result = get_longest_path(node)
        assert result >= 1

    def test_cycle_does_not_infinite_recurse(self):
        a = TopologyNode("A")
        b = TopologyNode("B")
        a.connected_to.append((b, AbstractRelation("R1", "A", "B")))
        b.connected_to.append((a, AbstractRelation("R2", "B", "A")))
        assert get_longest_path(a) == 2


# ════════════════════════════════════════════════
#  12. N + two relations (same pair, same direction)
# ════════════════════════════════════════════════

class TestMultipleRelationsSamePair:

    def test_two_relations_same_direction(self):
        """DOC-03: Person WORKS_AT Company AND Person FOUNDED Company."""
        t = _make_translator("Person,Company",
                             ["Person WORKS_AT Company",
                              "Person FOUNDED Company"])
        trees = t.get_topology_tree()
        assert len(trees) == 1
        assert trees[0].node_label == "Person"
        company_children = [c for c in trees[0].children if c.node_label == "Company"]
        assert len(company_children) >= 1


# ════════════════════════════════════════════════
#  13. Deep & wide topologies
# ════════════════════════════════════════════════

class TestDeepAndWide:

    def test_deep_chain_7_levels(self):
        """EDGE-07: A → B → C → D → E → F → G."""
        labels = "A,B,C,D,E,F,G"
        rels = ["A R1 B", "B R2 C", "C R3 D", "D R4 E", "E R5 F", "F R6 G"]
        t = _make_translator(labels, rels)
        trees = t.get_topology_tree()
        assert _labels(trees) == {"A", "B", "C", "D", "E", "F", "G"}
        assert _leaf_labels(trees) == {"G"}
        node = trees[0]
        depth = 1
        while node.children:
            assert len(node.children) == 1
            node = node.children[0]
            depth += 1
        assert depth == 7

    def test_wide_fork_6_children(self):
        """EDGE-08: Hub with 6 leaf children."""
        rels = [f"Hub R{i} L{i}" for i in range(1, 7)]
        labels = "Hub," + ",".join(f"L{i}" for i in range(1, 7))
        t = _make_translator(labels, rels)
        trees = t.get_topology_tree()
        assert len(trees) == 1
        assert len(trees[0].children) == 6
        assert NodeRole.FORK in trees[0].roles
        for child in trees[0].children:
            assert NodeRole.LEAF in child.roles


# ════════════════════════════════════════════════
#  14. Multi-root convergence
# ════════════════════════════════════════════════

class TestMultiRoot:

    def test_two_roots_sharing_subtree(self):
        """EDGE-11: R1 → Shared, R2 → Shared, Shared → Leaf."""
        t = _make_translator("R1,R2,Shared,Leaf",
                             ["R1 A Shared", "R2 B Shared", "Shared C Leaf"])
        trees = t.get_topology_tree()
        assert _root_labels(trees) == {"R1", "R2"}
        for tree in trees:
            assert _labels([tree]).issuperset({"Shared", "Leaf"})


# ════════════════════════════════════════════════
#  15. Self-loop + outgoing edge (root detection)
# ════════════════════════════════════════════════

class TestSelfLoopRootDetection:

    def test_self_ref_plus_outgoing(self):
        """EDGE-12: A SELF_REF A, A CONNECTS B.
        A should still be root (self_refs == incoming_con_n)."""
        t = _make_translator("A,B", ["A SELF_REF A", "A CONNECTS B"])
        assert len(t.roots) == 1
        assert t.roots[0].node_lbl == "A"
        trees = t.get_topology_tree()
        assert len(trees) == 1
        root = trees[0]
        assert root.node_label == "A"
        assert root.cycle_type == CycleType.SAME_TYPE
        b_children = [c for c in root.children if c.node_label == "B"]
        assert len(b_children) >= 1


# ════════════════════════════════════════════════
#  16. Bidirectional + extension
# ════════════════════════════════════════════════

class TestBidirectionalExtension:

    def test_bidirectional_pair_plus_outgoing(self):
        """EDGE-13: Person ↔ Company, Company → City."""
        t = _make_translator("Person,Company,City",
                             ["Person WORKS_AT Company",
                              "Company EMPLOYS Person",
                              "Company IN City"])
        trees = t.get_topology_tree()
        assert _labels(trees) == {"Person", "Company", "City"}


# ════════════════════════════════════════════════
#  17. Realistic schema (integration)
# ════════════════════════════════════════════════

class TestRealisticSchema:

    def test_six_types_seven_relations(self):
        """EDGE-14: Person, Company, City, Country, Department, Project
        with chains, forks, and a diamond."""
        t = _make_translator(
            "Person,Company,City,Country,Department,Project",
            [
                "Person WORKS_AT Company",
                "Person IN_DEPT Department",
                "Department PART_OF Company",
                "Company LOCATED_IN City",
                "City IN Country",
                "Person ASSIGNED_TO Project",
                "Project OWNED_BY Company",
            ],
        )
        trees = t.get_topology_tree()
        assert _labels(trees) == {"Person", "Company", "City", "Country",
                                  "Department", "Project"}
        assert len(trees) == 1
        assert trees[0].node_label == "Person"
        assert NodeRole.ROOT in trees[0].roles
        country_nodes = _find_all(trees, "Country")
        assert any(NodeRole.LEAF in c.roles for c in country_nodes)


# ════════════════════════════════════════════════
#  18. TopologyTranslator internal state
# ════════════════════════════════════════════════

class TestTranslatorInternals:

    def test_topology_detector_finds_all_relations(self):
        t = _make_translator("A,B,C", ["A R1 B", "B R2 C"])
        assert len(t.relations) == 2

    def test_topology_detector_sorts_by_outgoing(self):
        """top should be sorted by descending outgoing edge count."""
        t = _make_translator("A,B,C", ["A R1 B", "A R2 C"])
        assert t.top[0].node_lbl == "A"

    def test_roots_property(self):
        t = _make_translator("A,B,C", ["A R1 B", "A R2 C"])
        roots = t.roots
        assert len(roots) == 1
        assert roots[0].node_lbl == "A"

    def test_empty_data_returns_empty_tree(self):
        """Translator with no nodes should return empty list."""
        t = _make_translator("", [])
        trees = t.get_topology_tree()
        assert trees == []
