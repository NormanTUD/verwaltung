import pytest
from api.topology_detector import TopologyTranslator
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
        # Bypass the real Relationship.__init__ which expects a Graph
        self._nodes = (start_node, end_node)
        self._type = rel_type
        self._labels = frozenset()  # Relationships don't have labels in the same sense

    @property
    def nodes(self):
        return self._nodes


class FakeRecord:
    """Mimics neo4j.Record — iterable over its elements."""
    def __init__(self, *elements):
        self._elements = list(elements)

    def __iter__(self):
        return iter(self._elements)


@pytest.fixture
def diamond_topology_data():
    person = FakeNode("person")
    movie = FakeNode("movie")
    book = FakeNode("book")

    likes = FakeRelationship(person, movie, "likes")
    likes2 = FakeRelationship(person, book, "likes")
    written = FakeRelationship(movie, person, "written_by")
    written2 = FakeRelationship(book, person, "written_by")

    data = [FakeRecord(person, likes, movie),
            FakeRecord(person, likes2, book),
            FakeRecord(book, written2, person),
            FakeRecord(movie, written, person)
    ]

    return data

class TestTopology():
    def test_diamond(self, diamond_topology_data):
        tt = TopologyTranslator(diamond_topology_data)
        tt.print_topology()

if __name__ == "__main__":
    TT = TestTopology()
    d = diamond_topology_data()
    TT.test_diamond(d)



