import pytest
from api.topology_detector import TopologyTranslator, NodeRole, CycleType, TopologyTree
from neo4j.graph import Relationship

class FakeNode:
    """Mimics neo4j.graph.Node with just the .labels interface."""
    def __init__(self, label: str):
        self.labels = frozenset([label])

class FakeRelationship(Relationship):
    """
    Subclass of neo4j.graph.Relationship so isinstance() checks pass.
    We override __init__ to avoid needing a real Graph object.
    """
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


@pytest.fixture
def diamond_topology_data():
    """
    TRUE diamond shape:
        root ──has──→ movie ──written_by──→ person
          └───has──→ book  ──written_by──┘
    One root forks into two paths that converge on person.
    """
    root = FakeNode("root")
    person = FakeNode("person")
    movie = FakeNode("movie")
    book = FakeNode("book")

    has_movie = FakeRelationship(root, movie, "has")
    has_book = FakeRelationship(root, book, "has")
    written = FakeRelationship(movie, person, "written_by")
    written2 = FakeRelationship(book, person, "written_by")

    data = [
        FakeRecord(root, has_movie, movie),
        FakeRecord(root, has_book, book),
        FakeRecord(movie, written, person),
        FakeRecord(book, written2, person),
    ]
    return data


class TestDiamondTopology:

    def test_base_metadata(self, diamond_topology_data):
        tt = TopologyTranslator(diamond_topology_data)
        assert len(tt.top) == 4
        roots = tt.roots
        assert len(roots) == 1
        assert roots[0].node_lbl == "root"
        trees = tt.get_topology_tree()
        assert len(trees) == 1

    def test_root_is_fork(self, diamond_topology_data):
        """Root has two children (movie, book) so it must be FORK"""
        tt = TopologyTranslator(diamond_topology_data)
        tree = tt.get_topology_tree()[0]
        assert tree.node_label == "root"
        assert NodeRole.ROOT in tree.roles
        assert NodeRole.FORK in tree.roles
        assert tree.cycle_type == CycleType.NONE
        assert tree.relation_from_parent is None

    def test_root_children_are_movie_and_book(self, diamond_topology_data):
        tt = TopologyTranslator(diamond_topology_data)
        tree = tt.get_topology_tree()[0]
        assert len(tree.children) == 2
        child_labels = {c.node_label for c in tree.children}
        assert child_labels == {"movie", "book"}

    def test_middle_nodes_are_chain(self, diamond_topology_data):
        """movie and book each have exactly one child → CHAIN"""
        tt = TopologyTranslator(diamond_topology_data)
        tree = tt.get_topology_tree()[0]
        for child in tree.children:
            assert child.node_label in ("movie", "book")
            assert NodeRole.CHAIN in child.roles
            assert child.cycle_type == CycleType.NONE
            assert len(child.children) == 1

    def test_middle_nodes_have_correct_relation(self, diamond_topology_data):
        tt = TopologyTranslator(diamond_topology_data)
        tree = tt.get_topology_tree()[0]
        for child in tree.children:
            assert child.relation_from_parent is not None
            assert child.relation_from_parent.label == "has"
            assert child.relation_from_parent.from_node_type == "root"

    def test_person_appears_in_both_branches(self, diamond_topology_data):
        """THE defining diamond property: per-path ancestors (with backtracking)
        means person is NOT deduplicated — it appears under both movie AND book."""
        tt = TopologyTranslator(diamond_topology_data)
        tree = tt.get_topology_tree()[0]

        person_occurrences = []
        for child in tree.children:
            assert len(child.children) == 1
            grandchild = child.children[0]
            assert grandchild.node_label == "person"
            person_occurrences.append(grandchild)

        assert len(person_occurrences) == 2  # once under movie, once under book

    def test_person_is_leaf(self, diamond_topology_data):
        tt = TopologyTranslator(diamond_topology_data)
        tree = tt.get_topology_tree()[0]
        for child in tree.children:
            person = child.children[0]
            assert NodeRole.LEAF in person.roles
            assert len(person.children) == 0

    def test_person_has_correct_relation(self, diamond_topology_data):
        tt = TopologyTranslator(diamond_topology_data)
        tree = tt.get_topology_tree()[0]
        for child in tree.children:
            person = child.children[0]
            assert person.relation_from_parent.label == "written_by"
            assert person.relation_from_parent.to_node_type == "person"

    def test_no_cycles_anywhere(self, diamond_topology_data):
        """A diamond has NO cycles — convergence is not a cycle."""
        tt = TopologyTranslator(diamond_topology_data)
        tree = tt.get_topology_tree()[0]

        def assert_no_cycles(node: TopologyTree):
            assert node.cycle_type != CycleType.CROSS_TYPE, \
                f"{node.node_label} incorrectly marked as CROSS_TYPE"
            assert node.cycle_type != CycleType.SAME_TYPE, \
                f"{node.node_label} incorrectly marked as SAME_TYPE"
            assert node.same_type_info is None, \
                f"{node.node_label} should have no same_type_info"
            for child in node.children:
                assert_no_cycles(child)

        assert_no_cycles(tree)

    def test_full_tree_shape(self, diamond_topology_data):
        """Integration test: assert the complete tree structure at once."""
        tt = TopologyTranslator(diamond_topology_data)
        tree = tt.get_topology_tree()[0]

        # root (FORK) → [movie (CHAIN), book (CHAIN)] → each has [person (LEAF)]
        assert tree.node_label == "root"
        assert len(tree.children) == 2

        by_label = {c.node_label: c for c in tree.children}
        assert "movie" in by_label
        assert "book" in by_label

        movie_person = by_label["movie"].children[0]
        book_person = by_label["book"].children[0]

        assert movie_person.node_label == "person"
        assert book_person.node_label == "person"
        # These are separate TopologyTree instances — same label, different objects
        assert movie_person is not book_person
        assert movie_person.children == []
        assert book_person.children == []
