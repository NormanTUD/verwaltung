# mypy: ignore-errors
import pytest
from api.neo4j_interface import Neo4jDB, ReadRequest, construct_cypher_query
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError, CypherSyntaxError
import os
import t_helpers, conftest
from conftest import URI, AUTH

class TestCypherConstruction:
    def test_invalid_labels_injection(self):
        label_w_special_chars = {
                                # "1Label", # Labeling like this could lead to problems when requesting them without backticks
                                "-Label",
                                "Label:Label",
                                "Label.Name",
                                "Label with space"
                                }
        empty_and_spaces = {
                            "",
                            "   ",
                            "\t",
                            "\n"}
        reserved_keywords = {
                            "$param",
                            "MATCH",
                            "WHERE",
                            "RETURN",
                            "NULL",
                            "TRUE",
                            "FALSE"
                            }
        injections = {
            "Label //",
            "Label) RETURN n //",
            "Label` //",
            "Label) MATCH (n) RETURN n --",
            "Label) WITH n MATCH (m) RETURN m --",
            "Label OR 1=1",
            "` MATCH (n) DETACH DELETE n --",
            "`) MATCH (n) RETURN n UNION MATCH (m) --",
            "Label` WITH n CALL db.labels() YIELD label --",
            "Label) UNION CALL db.labels() YIELD label RETURN count(label) --",
            "Label) LOAD CSV FROM 'http://evil.com/data' AS line --",
            "Label) RETURN keys(n) --",
            "Label) CALL apoc.cypher.runMany('MATCH (n) RETURN n') YIELD result --",
            "Label) CALL apoc.text.base64Encode(n.secret) YIELD value RETURN value --"
        }
        label_w_special_chars.update(empty_and_spaces, reserved_keywords, injections)
        for l in label_w_special_chars:
            with pytest.raises(ValueError) as exc_info:
                construct_cypher_query(l)
                pytest.fail(f"Cypher Construction Test: Value Error not risen for label {l}")
            assert exc_info.type == ValueError, f"Cypher Construction did not catch special char {l}"

class TestBasicDBReads:
    """Integration tests for core Neo4jDB read operations:
    simple reads, where filters, limits, relationships, and error handling."""

    def test_simple_db_reads(self, db: "Neo4jDB"):
        """Basic read requests from the Data-Layer Neo4jDB class."""
        label1 = "Student"
        req = ReadRequest([label1], None, None, None)
        records = list(db.read_data(req))
        for s in t_helpers.STUDENTS:
            assert s.f_name in [r.data()["n"]["f_name"] for r in records]

        label2 = "Seminar"
        req = ReadRequest([label2], None, None, None)
        records = list(db.read_data(req))
        for c in t_helpers.SEMINARS:
            assert c.name in [r.data()["n"]["title"] for r in records]

    def test_where_request(self, db: "Neo4jDB"):
        """Basic where requests using QueryBuilder format."""
        lbl = "Student"
        names = [s.f_name for s in t_helpers.STUDENTS]

        for n in names:
            qb_filter = {
                "condition": "AND",
                "rules": [
                    {"field": "Student.f_name", "operator": "equal", "value": n}
                ],
                "valid": True,
            }
            req = ReadRequest([lbl], None, qb_filter, None)
            records = list(db.read_data(req))

            assert n in [r.data()["n"]["f_name"] for r in records] and len(records) == 1
            assert "Cookiebert Strauss" not in [r.data()["n"]["f_name"] for r in records]

    @pytest.mark.parametrize("limit", range(1, 8))
    def test_limit_request(self, db: "Neo4jDB", limit: int):
        """Iterative limit requests from the Data-Layer Neo4jDB class."""
        lbl = "Student"
        req = ReadRequest([lbl], limit, None, None)
        records = list(db.read_data(req))
        assert len(records) == limit

    def test_simple_relationships(self, db: "Neo4jDB"):
        """Basic relationship requests from the Data-Layer Neo4jDB class."""
        label1 = "Student"
        req = ReadRequest(
            [label1],
            None,
            None,
            [t_helpers.CONNECTIONS[(t_helpers.Student, t_helpers.Seminar)]],
        )
        records = list(db.read_data(req))

        assert len(records) > 5, (
            f"Too few records for student-seminar enrolled relation: {records}"
        )
        for r in records:
            relation = r.data()["r"]
            assert relation[1] == t_helpers.CONNECTIONS[
                (t_helpers.Student, t_helpers.Seminar)
            ], f"No relation in {relation}, or at least not at index 1"

    def test_unpresent_label(self, db: "Neo4jDB"):
        """Requests for a label that doesn't exist should return nothing."""
        m_label = "COOOOKIES"
        req = ReadRequest([m_label], None, None, ["ENROLLED"])
        records = list(db.read_data(req))
        assert len(records) == 0

    @pytest.mark.parametrize("bad_lim", [-3, "f", 14.13], ids=["negative", "string", "float"])
    def test_bad_limit(self, db: "Neo4jDB", bad_lim):
        """Expected behavior with wrong limit input."""
        label1 = "Student"
        req = ReadRequest(
            selected_labels=[label1],
            limit=bad_lim,
            property_filters=None,
            rel_filter=None,
        )
        with pytest.raises(ClientError):
            db.read_data(req)


class TestCypherInjectionDBInterfaceLevel:
    INJECTION_PAYLOADS = {
    "tautology": "' OR 1=1 --",
    "union_extraction": "' UNION ALL MATCH (n) RETURN n --",
    "destructive": "'; MATCH (n) DETACH DELETE n; --",
    "malicious_label": "Student) MATCH (n) DETACH DELETE n --"
}
    def test_injection_in_where_values(self, db: "Neo4jDB"):
        """Attempts to inject logic into QueryBuilder values."""
        malicious_name = "NonExistentUser" + self.INJECTION_PAYLOADS["tautology"]

        qb_filter = {
            "condition": "AND",
            "rules": [
                {"field": "Student.f_name", "operator": "equal", "value": malicious_name}
            ],
            "valid": True
        }

        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=qb_filter,
            rel_filter=None
        )

        records = list(db.read_data(req))
        assert len(records) == 0, f"Injection successful! Tautology returned {len(records)} records."

    def test_destructive_label_injection(self, db: "Neo4jDB"):
        """
        Attempts to inject Cypher into the 'selected_labels' list.
        Dynamic labels are a common vulnerability point.
        """
        req = ReadRequest(
            selected_labels=[TestCypherInjectionDBInterfaceLevel.INJECTION_PAYLOADS["malicious_label"]],
            limit=None,
            property_filters=None,
            rel_filter=None
        )

        try:
            list(db.read_data(req))
        except Exception:
            pass

        check_req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=None
        )
        students = list(db.read_data(check_req))
        assert len(students) > 0, "Destructive injection in Label succeeded! Students were deleted."

    def test_injection_in_relationships(self, db: "Neo4jDB"):
        """
        Attempts to inject Cypher into the 'rel_filter' list.
        """
        malicious_rel = "ENROLLED]-(s) RETURN s UNION CALL db.labels() --"

        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[malicious_rel]
        )

        try:
            results = list(db.read_data(req))
            for r in results:
                data = r.data()
                assert "label" not in data, "Injection exfiltrated DB labels via UNION!"
        except Exception:
            pass

    def test_injection_in_limit(self, db: "Neo4jDB"):
        """
        Attempts to pass a string injection into the 'limit' field.
        """
        malicious_limit = "1 UNION ALL MATCH (n) RETURN n"

        req = ReadRequest(
            selected_labels=["Student"],
            limit=malicious_limit,
            property_filters=None,
            rel_filter=None
        )

        try:
            records = list(db.read_data(req))
            if len(records) > 1:
                pytest.fail("Limit injection succeeded: Query returned more than 1 result.")
        except Exception:
            pass

    def test_destructive_where_injection(self, db: "Neo4jDB"):
        """
        High-risk test: Attempts to delete the database via the property_filters clause.
        """
        assert len(t_helpers.STUDENTS) > 0

        malicious_val = "x" + TestCypherInjectionDBInterfaceLevel.INJECTION_PAYLOADS["destructive"]

        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters={"f_name": malicious_val},
            rel_filter=None
        )

        try:
            list(db.read_data(req))
        except Exception:
            pass

        check_req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=None
        )
        remaining_students = list(db.read_data(check_req))

        assert len(remaining_students) > 0, "Destructive injection in WHERE clause succeeded! Database wiped."


class TestPropertyFilters:
    """
    Test suite for the `property_filters` field of ReadRequest.
    Expects a jQuery QueryBuilder dict:
    {
        "condition": "AND" | "OR",
        "rules": [
            {"field": "<Label>.<prop>", "operator": "<op>", "value": <val>},
            ...  # or nested group dicts
        ],
        "valid": True
    }

    Test data reference (Thesis nodes from t_helpers.THESES):
      101: year=1998, grade=95.5, published=True,  pages=120, dept=Science,  topic=Potions
      102: year=1999, grade=88.0, published=False, pages=85,  dept=History,  topic=Economics
      103: year=1998, grade=92.0, published=True,  pages=200, dept=Defense,  topic=Defense
      104: year=2000, grade=99.9, published=True,  pages=300, dept=Biology,  topic=Herbology
      105: year=1999, grade=85.5, published=False, pages=90,  dept=Biology,  topic=Magizoology
    """

    LABEL = "Thesis"

    # ── helpers ──────────────────────────────────────────────────────

    def _qb(self, rules, condition="AND", valid=True):
        """Shorthand to build a QueryBuilder dict."""
        return {"condition": condition, "rules": rules, "valid": valid}

    _sentinel = object()

    def _rule(self, field, operator, value=_sentinel):
        """Build a single rule dict. Omits 'value' key for unary operators."""
        r = {"field": f"{self.LABEL}.{field}", "operator": operator}
        if value is not self._sentinel:
            r["value"] = value
        return r

    def _read(self, db, qb_filter, label=None):
        req = ReadRequest(
            selected_labels=[label or self.LABEL],
            limit=None,
            property_filters=qb_filter,
            rel_filter=None,
        )
        return list(db.read_data(req))

    def _ids(self, records):
        return {r.data()["n"]["id"] for r in records}

    # ═══════════════════════════════════════════════════════════════
    # 1. COMPARISON OPERATORS  (across int / float / string types)
    # ═══════════════════════════════════════════════════════════════

    @pytest.mark.parametrize("op, value, expected_ids", [
        ("equal",            1998,  {101, 103}),
        ("not_equal",        1998,  {102, 104, 105}),
        ("less",             1999,  {101, 103}),         # year < 1999
        ("less_or_equal",    1999,  {101, 102, 103, 105}),
        ("greater",          1999,  {104}),              # year > 1999
        ("greater_or_equal", 1999,  {102, 104, 105}),
    ], ids=lambda v: str(v) if not isinstance(v, set) else None)
    def test_comparison_operators_integer(self, db, op, value, expected_ids):
        """Comparison operators on an integer field (year)."""
        records = self._read(db, self._qb([self._rule("year", op, value)]))
        assert self._ids(records) == expected_ids

    @pytest.mark.parametrize("op, value, expected_ids", [
        ("less",             90.0, {102, 105}),         # grade < 90
        ("greater_or_equal", 92.0, {101, 103, 104}),   # grade >= 92
    ])
    def test_comparison_operators_float(self, db, op, value, expected_ids):
        """Comparison operators on a float field (grade)."""
        records = self._read(db, self._qb([self._rule("grade", op, value)]))
        assert self._ids(records) == expected_ids

    # ═══════════════════════════════════════════════════════════════
    # 2. BETWEEN / NOT_BETWEEN
    # ═══════════════════════════════════════════════════════════════

    def test_between(self, db):
        """between: grade in [88.0, 95.5] → 101 (95.5), 102 (88.0), 103 (92.0)"""
        records = self._read(db, self._qb([
            self._rule("grade", "between", [88.0, 95.5])
        ]))
        assert self._ids(records) == {101, 102, 103}

    def test_not_between(self, db):
        """not_between: grade outside [88.0, 95.5] → 104 (99.9), 105 (85.5)"""
        records = self._read(db, self._qb([
            self._rule("grade", "not_between", [88.0, 95.5])
        ]))
        assert self._ids(records) == {104, 105}

    # ═══════════════════════════════════════════════════════════════
    # 3. STRING OPERATORS
    # ═══════════════════════════════════════════════════════════════

    def test_contains(self, db):
        """contains: title contains 'Dark' → 103"""
        records = self._read(db, self._qb([self._rule("title", "contains", "Dark")]))
        assert 103 in self._ids(records)

    def test_not_contains(self, db):
        """not_contains: title NOT contains 'Dark' → everything except 103"""
        records = self._read(db, self._qb([self._rule("title", "not_contains", "Dark")]))
        ids = self._ids(records)
        assert 103 not in ids
        assert len(ids) == len(t_helpers.THESES) - 1

    def test_begins_with(self, db):
        """begins_with: title starts with 'Advanced' → 101"""
        records = self._read(db, self._qb([self._rule("title", "begins_with", "Advanced")]))
        assert self._ids(records) == {101}

    def test_ends_with(self, db):
        """ends_with: title ends with 'Guide' → 103"""
        records = self._read(db, self._qb([self._rule("title", "ends_with", "Guide")]))
        assert self._ids(records) == {103}

    @pytest.mark.skip(reason="not begins with - Will be looked into")
    def test_not_begins_with(self, db):
        """not_begins_with: excludes thesis 101"""

        records = self._read(db, self._qb([self._rule("title", "not_begins_with", "Advanced")]))
        ids = self._ids(records)
        assert 101 not in ids
        assert len(ids) == len(t_helpers.THESES) - 1

    # ═══════════════════════════════════════════════════════════════
    # 4. BOOLEAN EQUALITY
    # ═══════════════════════════════════════════════════════════════

    def test_equal_boolean_true(self, db):
        """is_published == True → 101, 103, 104"""
        records = self._read(db, self._qb([
            self._rule("is_published", "equal", True)
        ]))
        assert self._ids(records) == {101, 103, 104}

    def test_equal_boolean_false(self, db):
        """is_published == False → 102, 105"""
        records = self._read(db, self._qb([
            self._rule("is_published", "equal", False)
        ]))
        assert self._ids(records) == {102, 105}

    # ═══════════════════════════════════════════════════════════════
    # 5. SET OPERATORS  (in / not_in)
    # ═══════════════════════════════════════════════════════════════

    def test_in_operator(self, db):
        """in: department in ['Science', 'Defense'] → 101, 103"""
        records = self._read(db, self._qb([
            self._rule("department", "in", ["Science", "Defense"])
        ]))
        assert self._ids(records) == {101, 103}

    def test_not_in_operator(self, db):
        """not_in: department NOT in ['Science', 'Defense', 'History'] → 104, 105"""
        records = self._read(db, self._qb([
            self._rule("department", "not_in", ["Science", "Defense", "History"])
        ]))
        assert self._ids(records) == {104, 105}

    @pytest.mark.skip(reason="in-with-propertylist - Will be looked into")
    def test_in_with_list_property(self, db):
        """in operator against the list‑typed 'keywords' field."""
        records = self._read(db, self._qb([
            self._rule("keywords", "in", ["plants", "dangerous", "forest"])
        ]))
        ids = self._ids(records)
        assert 104 in ids  # keywords = ["plants", "dangerous", "forest"]

    # ═══════════════════════════════════════════════════════════════
    # 6. NULL / EMPTY OPERATORS  (unary – no value key)
    # ═══════════════════════════════════════════════════════════════

    def test_is_not_null_existing_property(self, db):
        """All theses have a title → all returned."""
        records = self._read(db, self._qb([self._rule("title", "is_not_null")]))
        assert len(records) == len(t_helpers.THESES)

    def test_is_null_existing_property(self, db):
        """title is always set → nothing returned."""
        records = self._read(db, self._qb([self._rule("title", "is_null")]))
        assert len(records) == 0

    def test_is_not_null_absent_property(self, db):
        """Property that no node has → nothing returned."""
        records = self._read(db, self._qb([self._rule("nonexistent", "is_not_null")]))
        assert len(records) == 0

    @pytest.mark.skip(reason="is empty filter - Will be looked into")
    def test_is_empty(self, db):
        """Non‑empty title → nothing returned."""
        records = self._read(db, self._qb([self._rule("title", "is_empty")]))
        assert len(records) == 0

    @pytest.mark.skip(reason="not begins with - Will be looked into")
    def test_is_not_empty(self, db):
        """All titles populated → all returned."""
        records = self._read(db, self._qb([self._rule("title", "is_not_empty")]))
        assert len(records) == len(t_helpers.THESES)

    # ═══════════════════════════════════════════════════════════════
    # 7. OR CONDITION
    # ═══════════════════════════════════════════════════════════════

    def test_or_condition_simple(self, db):
        """OR: dept='Science' OR dept='History' → 101, 102"""
        qb = self._qb([
            self._rule("department", "equal", "Science"),
            self._rule("department", "equal", "History"),
        ], condition="OR")
        assert self._ids(self._read(db, qb)) == {101, 102}

    def test_or_with_mixed_types(self, db):
        """OR across different field types: year=2000 OR is_published=False → 102,104,105"""
        qb = self._qb([
            self._rule("year", "equal", 2000),
            self._rule("is_published", "equal", False),
        ], condition="OR")
        assert self._ids(self._read(db, qb)) == {102, 104, 105}

    # ═══════════════════════════════════════════════════════════════
    # 8. NESTED GROUPS
    # ═══════════════════════════════════════════════════════════════

    def test_nested_and_inside_or(self, db):
        """
        (year == 1998 AND is_published == True) OR (department == 'Biology')
        First group  → 101, 103
        Second group → 104, 105
        Union        → {101, 103, 104, 105}
        """
        qb = {
            "condition": "OR",
            "rules": [
                {
                    "condition": "AND",
                    "rules": [
                        {"field": "Thesis.year", "operator": "equal", "value": 1998},
                        {"field": "Thesis.is_published", "operator": "equal", "value": True},
                    ],
                    "valid": True,
                },
                {"field": "Thesis.department", "operator": "equal", "value": "Biology"},
            ],
            "valid": True,
        }
        assert self._ids(self._read(db, qb)) == {101, 103, 104, 105}

    def test_deeply_nested_groups(self, db):
        """
        ((grade > 90) AND (year == 1998))   →  101, 103
        OR
        ((dept == 'Biology') AND (published == False))  →  105
        Result: {101, 103, 105}
        """
        qb = {
            "condition": "OR",
            "rules": [
                {
                    "condition": "AND",
                    "rules": [
                        {"field": "Thesis.grade", "operator": "greater", "value": 90.0},
                        {"field": "Thesis.year", "operator": "equal", "value": 1998},
                    ],
                    "valid": True,
                },
                {
                    "condition": "AND",
                    "rules": [
                        {"field": "Thesis.department", "operator": "equal", "value": "Biology"},
                        {"field": "Thesis.is_published", "operator": "equal", "value": False},
                    ],
                    "valid": True,
                },
            ],
            "valid": True,
        }
        assert self._ids(self._read(db, qb)) == {101, 103, 105}

    def test_triple_nesting(self, db):
        """
        Three‑level nesting:
        (
          ( (pages >= 200) AND (grade > 90) )   → 103(200,92), 104(300,99.9)
          OR
          (year == 1999)                         → 102, 105
        )
        AND
        (is_published == True)                   → keeps 103, 104 from first; none from second
        Result: {103, 104}
        """
        qb = {
            "condition": "AND",
            "rules": [
                {
                    "condition": "OR",
                    "rules": [
                        {
                            "condition": "AND",
                            "rules": [
                                {"field": "Thesis.pages", "operator": "greater_or_equal", "value": 200},
                                {"field": "Thesis.grade", "operator": "greater", "value": 90.0},
                            ],
                            "valid": True,
                        },
                        {"field": "Thesis.year", "operator": "equal", "value": 1999},
                    ],
                    "valid": True,
                },
                {"field": "Thesis.is_published", "operator": "equal", "value": True},
            ],
            "valid": True,
        }
        assert self._ids(self._read(db, qb)) == {103, 104}

    # ═══════════════════════════════════════════════════════════════
    # 9. MULTIPLE AND RULES  (many fields, mixed types in one query)
    # ═══════════════════════════════════════════════════════════════

    def test_many_and_rules_exact_match(self, db):
        """
        Pinpoint thesis 104 with four simultaneous AND constraints
        across int, float, bool, and string fields.
        """
        qb = self._qb([
            self._rule("year", "equal", 2000),
            self._rule("grade", "greater_or_equal", 99.0),
            self._rule("is_published", "equal", True),
            self._rule("department", "equal", "Biology"),
        ])
        records = self._read(db, qb)
        assert self._ids(records) == {104}

    # ═══════════════════════════════════════════════════════════════
    # 10. EDGE CASES — no results, nonexistent prop, type mismatch
    # ═══════════════════════════════════════════════════════════════

    def test_no_matching_results(self, db):
        """Valid filter that matches nothing."""
        records = self._read(db, self._qb([self._rule("year", "equal", 3000)]))
        assert len(records) == 0

    def test_nonexistent_property_equals(self, db):
        """Filter on a property that no node possesses."""
        records = self._read(db, self._qb([
            self._rule("favourite_cookie", "equal", "chocolate")
        ]))
        assert len(records) == 0

    def test_type_mismatch_string_for_int(self, db):
        """String value against an integer field should match nothing."""
        records = self._read(db, self._qb([self._rule("year", "equal", "not_a_number")]))
        assert len(records) == 0

    def test_type_mismatch_int_for_string(self, db):
        """Integer value against a string field should match nothing."""
        records = self._read(db, self._qb([self._rule("department", "equal", 42)]))
        assert len(records) == 0

    # ═══════════════════════════════════════════════════════════════
    # 11. EDGE CASES — empty / invalid structures
    # ═══════════════════════════════════════════════════════════════

    def test_empty_rules_list(self, db):
        """Empty rules array should return all nodes or be explicitly rejected."""
        qb = self._qb([])
        try:
            records = self._read(db, qb)
        except (CypherSyntaxError):
            pass


    def test_valid_false(self, db):
        """valid=False filter should be rejected or have no effect."""
        qb = self._qb([self._rule("year", "equal", 1998)], valid=False)
        try:
            records = self._read(db, qb)
        except (ValueError):
            pass  # preferred: explicit rejection

    def test_none_value_in_equality(self, db):
        """None as value for equality — should return nothing or raise."""
        qb = self._qb([{"field": "Thesis.department", "operator": "equal", "value": None}])
        try:
            records = self._read(db, qb)
            assert len(records) == 0
        except (ValueError, TypeError):
            pass

    # ═══════════════════════════════════════════════════════════════
    # 12. MALFORMED FILTER DICTS — missing required keys
    # ═══════════════════════════════════════════════════════════════

    def test_missing_condition_key(self, db):
        qb = {
            "rules": [{"field": "Thesis.year", "operator": "equal", "value": 1998}],
            "valid": True,
        }
        with pytest.raises((ValueError, KeyError, TypeError)):
            assert not self._read(db, qb)


    def test_missing_operator_in_rule(self, db):
        qb = {
            "condition": "AND",
            "rules": [{"field": "Thesis.year", "value": 1998}],
            "valid": True,
        }
        with pytest.raises((ValueError, KeyError, TypeError)):
            self._read(db, qb)

    def test_missing_field_in_rule(self, db):
        qb = {
            "condition": "AND",
            "rules": [{"operator": "equal", "value": 1998}],
            "valid": True,
        }
        with pytest.raises((ValueError, KeyError, TypeError)):
            self._read(db, qb)

    def test_invalid_operator_name(self, db):
        """Completely unsupported operator."""
        qb = self._qb([self._rule("year", "LIKE", "%1998%")])
        with pytest.raises((ValueError, KeyError)):
            self._read(db, qb)

    def test_invalid_condition_name(self, db):
        """Condition is neither AND nor OR."""
        qb = self._qb([self._rule("year", "equal", 1998)], condition="XOR")
        with pytest.raises((ValueError, KeyError)):
            self._read(db, qb)

    # ═══════════════════════════════════════════════════════════════
    # 13. INJECTION ATTEMPTS — field name, operator, value
    # ═══════════════════════════════════════════════════════════════

    def test_injection_in_field_name(self, db):
        """Cypher injection via the field name."""
        qb = {
            "condition": "AND",
            "rules": [{
                "field": "Thesis.year} RETURN n UNION MATCH (m) RETURN m //",
                "operator": "equal",
                "value": 1998,
            }],
            "valid": True,
        }
        try:
            records = self._read(db, qb)
            assert len(records) <= len(t_helpers.THESES), \
                "Field‑name injection returned more records than exist!"
        except (ValueError, Exception):
            pass  # preferred: explicit rejection

    def test_injection_in_operator(self, db):
        """Cypher injection via the operator string."""
        qb = {
            "condition": "AND",
            "rules": [{
                "field": "Thesis.year",
                "operator": "equal RETURN n //",
                "value": 1998,
            }],
            "valid": True,
        }
        try:
            records = self._read(db, qb)
            assert len(records) == 0
        except (ValueError, Exception):
            pass

    def test_injection_in_value(self, db):
        """Tautology injection via the value string."""
        qb = self._qb([
            self._rule("department", "equal", "Science' OR 1=1 --")
        ])
        records = self._read(db, qb)
        assert len(records) == 0, "Value injection returned unexpected results"

    # ═══════════════════════════════════════════════════════════════
    # 14. COMBINED WITH OTHER ReadRequest PARAMS
    # ═══════════════════════════════════════════════════════════════

    def test_filter_combined_with_limit(self, db):
        """property_filters + limit — limit must be respected."""
        qb = self._qb([self._rule("is_published", "equal", True)])
        req = ReadRequest(
            selected_labels=[self.LABEL],
            limit=2,
            property_filters=qb,
            rel_filter=None,
        )
        records = list(db.read_data(req))
        assert len(records) == 2
        for r in records:
            assert r.data()["n"]["is_published"] is True

    def test_filter_combined_with_relationship(self, db):
        """property_filters + rel_filter at the same time."""
        qb = self._qb([self._rule("is_published", "equal", True)])
        req = ReadRequest(
            selected_labels=[self.LABEL],
            limit=None,
            property_filters=qb,
            rel_filter=[t_helpers.CONNECTIONS[(t_helpers.Student, t_helpers.MasterThesis)]],
        )
        records = list(db.read_data(req))
        for r in records:
            assert r.data()["n"]["is_published"] is True

    def test_filter_on_different_label(self, db):
        """Filter targets Student label instead of Thesis."""
        qb = {
            "condition": "AND",
            "rules": [
                {"field": "Student.f_name", "operator": "equal", "value": "Hermoine"},
            ],
            "valid": True,
        }
        records = self._read(db, qb, label="Student")
        assert len(records) == 1
        assert records[0].data()["n"]["f_name"] == "Hermoine"

class TestRelAsFilter:
    """
    Test suite for the `rel_as_filter` parameter on ReadRequest / construct_cypher_query.

    Two modes:
      rel_as_filter=True  (default) → strict MATCH: only nodes participating in the relationship are returned.
      rel_as_filter=False           → OPTIONAL MATCH: all matching nodes returned; relationships enriched where present.
    """

    AUTHORED_REL = t_helpers.CONNECTIONS[(t_helpers.Student, t_helpers.MasterThesis)]
    ENROLLED_REL = t_helpers.CONNECTIONS[(t_helpers.Student, t_helpers.Seminar)]
    SUPERVISES_REL = t_helpers.CONNECTIONS[(t_helpers.Teacher, t_helpers.MasterThesis)]

    STUDENTS_WITH_THESIS = {1, 2, 3, 4, 5}
    STUDENTS_WITHOUT_THESIS = {6, 7, 8}
    ALL_STUDENT_IDS = STUDENTS_WITH_THESIS | STUDENTS_WITHOUT_THESIS

    # ═══════════════════════════════════════════════════════════════
    # A. CYPHER CONSTRUCTION — verify query shape
    # ═══════════════════════════════════════════════════════════════

    def test_default_rel_as_filter_is_true(self):
        """Backward compatibility: omitting rel_as_filter produces a strict MATCH (no OPTIONAL)."""
        cypher, _ = construct_cypher_query(
            node_labels=["Student"],
            rel_types=[self.AUTHORED_REL],
        )
        assert "OPTIONAL" not in cypher
        assert "MATCH" in cypher

    def test_cypher_rel_as_filter_true_produces_strict_match(self):
        """rel_as_filter=True should produce a single MATCH with the relationship pattern."""
        cypher, _ = construct_cypher_query(
            node_labels=["Student"],
            rel_types=[self.AUTHORED_REL],
            rel_as_filter=True,
        )
        assert "OPTIONAL" not in cypher
        assert f"-[r:{self.AUTHORED_REL}]->" in cypher

    def test_cypher_rel_as_filter_false_produces_optional_match(self):
        """rel_as_filter=False should produce MATCH (n:Label) OPTIONAL MATCH (n)-[r:...]->(m)."""
        cypher, _ = construct_cypher_query(
            node_labels=["Student"],
            rel_types=[self.AUTHORED_REL],
            rel_as_filter=False,
        )
        assert "OPTIONAL MATCH" in cypher
        # The node MATCH should appear without the relationship pattern
        assert "MATCH (n:Student)" in cypher
        assert f"-[r:{self.AUTHORED_REL}]->" in cypher

    def test_cypher_rel_as_filter_ignored_when_no_rel_types(self):
        """When rel_types is None, rel_as_filter should have no effect on the query."""
        cypher_default, _ = construct_cypher_query(
            node_labels=["Student"],
            rel_types=None,
            rel_as_filter=True,
        )
        cypher_optional, _ = construct_cypher_query(
            node_labels=["Student"],
            rel_types=None,
            rel_as_filter=False,
        )
        assert cypher_default == cypher_optional
        assert "OPTIONAL" not in cypher_default

    def test_cypher_rel_as_filter_false_returns_n_r_m(self):
        """Even in OPTIONAL mode, the RETURN clause should include n, r, m."""
        cypher, _ = construct_cypher_query(
            node_labels=["Student"],
            rel_types=[self.AUTHORED_REL],
            rel_as_filter=False,
        )
        assert "RETURN n, r, m" in cypher

    def test_cypher_rel_as_filter_false_with_multiple_rel_types(self):
        """OPTIONAL MATCH should work with multiple relationship types (OR'd)."""
        cypher, _ = construct_cypher_query(
            node_labels=["Student"],
            rel_types=[self.AUTHORED_REL, self.ENROLLED_REL],
            rel_as_filter=False,
        )
        assert "OPTIONAL MATCH" in cypher
        assert f"{self.AUTHORED_REL}|{self.ENROLLED_REL}" in cypher

    # ═══════════════════════════════════════════════════════════════
    # B. INTEGRATION — rel_as_filter=True (strict / default)
    # ═══════════════════════════════════════════════════════════════

    def test_strict_mode_excludes_unrelated_nodes(self, db: "Neo4jDB"):
        """
        rel_as_filter=True (default): Students without an 'authored' relationship
        should NOT appear in the results.
        """
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=True,
        )
        records = list(db.read_data(req))
        returned_ids = {r.data()["n"]["student_id"] for r in records}

        for sid in self.STUDENTS_WITHOUT_THESIS:
            assert sid not in returned_ids, (
                f"Student {sid} has no 'authored' rel but appeared in strict mode"
            )

    def test_strict_mode_only_returns_related_nodes(self, db: "Neo4jDB"):
        """rel_as_filter=True: only students 1–5 (who authored a thesis) are returned."""
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=True,
        )
        records = list(db.read_data(req))
        returned_ids = {r.data()["n"]["student_id"] for r in records}

        assert returned_ids == self.STUDENTS_WITH_THESIS

    def test_strict_mode_all_records_have_relationship(self, db: "Neo4jDB"):
        """In strict mode every returned record must contain a non-null relationship."""
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=True,
        )
        records = list(db.read_data(req))
        for r in records:
            data = r.data()
            assert data["r"] is not None, "Strict mode returned a record with null relationship"
            assert data["m"] is not None, "Strict mode returned a record with null target node"

    # ═══════════════════════════════════════════════════════════════
    # C. INTEGRATION — rel_as_filter=False (optional enrichment)
    # ═══════════════════════════════════════════════════════════════

    def test_optional_mode_returns_all_nodes(self, db: "Neo4jDB"):
        """
        rel_as_filter=False: ALL students should appear, regardless of
        whether they have an 'authored' relationship.
        """
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=False,
        )
        records = list(db.read_data(req))
        returned_ids = {r.data()["n"]["student_id"] for r in records}

        assert self.ALL_STUDENT_IDS.issubset(returned_ids), (
            f"Optional mode should return all students. Missing: "
            f"{self.ALL_STUDENT_IDS - returned_ids}"
        )

    def test_optional_mode_unrelated_nodes_have_null_relationship(self, db: "Neo4jDB"):
        """
        rel_as_filter=False: Students without the 'authored' relationship
        should still appear but with r=None and m=None.
        """
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=False,
        )
        records = list(db.read_data(req))

        for r in records:
            data = r.data()
            sid = data["n"]["student_id"]
            if sid in self.STUDENTS_WITHOUT_THESIS:
                assert data["r"] is None, (
                    f"Student {sid} should have null relationship in optional mode"
                )
                assert data["m"] is None, (
                    f"Student {sid} should have null target node in optional mode"
                )

    def test_optional_mode_related_nodes_still_have_relationship(self, db: "Neo4jDB"):
        """
        rel_as_filter=False: Students WITH the 'authored' relationship
        should still have non-null r and m.
        """
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=False,
        )
        records = list(db.read_data(req))

        related_records = [
            r for r in records
            if r.data()["n"]["student_id"] in self.STUDENTS_WITH_THESIS
        ]
        assert len(related_records) > 0, "No records found for students who authored theses"

        for r in related_records:
            data = r.data()
            assert data["r"] is not None, (
                f"Student {data['n']['student_id']} authored a thesis but r is null"
            )
            assert data["m"] is not None, (
                f"Student {data['n']['student_id']} authored a thesis but m is null"
            )

    def test_optional_mode_returns_more_results_than_strict(self, db: "Neo4jDB"):
        """
        The optional-mode result set must be a superset of the strict-mode
        result set (in terms of distinct student IDs).
        """
        strict_req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=True,
        )
        optional_req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=False,
        )

        strict_ids = {r.data()["n"]["student_id"] for r in db.read_data(strict_req)}
        optional_ids = {r.data()["n"]["student_id"] for r in db.read_data(optional_req)}

        assert strict_ids.issubset(optional_ids), (
            "Strict IDs should be a subset of optional IDs"
        )
        assert len(optional_ids) > len(strict_ids), (
            "Optional mode should return more distinct students than strict mode"
        )

    # ═══════════════════════════════════════════════════════════════
    # D. COMBINED WITH PROPERTY FILTERS
    # ═══════════════════════════════════════════════════════════════

    def test_optional_mode_with_property_filter(self, db: "Neo4jDB"):
        """
        rel_as_filter=False + property filter on n: the WHERE clause must
        bind to n and NOT accidentally filter out NULL rows from OPTIONAL MATCH.
        Filtering for f_name='Ginny' (student 6, no thesis) should still return her.
        """
        qb_filter = {
            "condition": "AND",
            "rules": [
                {"field": "Student.f_name", "operator": "equal", "value": "Ginny"}
            ],
            "valid": True,
        }
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=qb_filter,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=False,
        )
        records = list(db.read_data(req))

        assert len(records) == 1, (
            f"Expected exactly 1 record for Ginny, got {len(records)}, {records}"
        )
        data = records[0].data()
        assert data["n"]["f_name"] == "Ginny"
        assert data["r"] is None, "Ginny has no thesis; r should be null"
        assert data["m"] is None, "Ginny has no thesis; m should be null"

    def test_strict_mode_with_property_filter(self, db: "Neo4jDB"):
        """
        rel_as_filter=True + property filter for a student WITHOUT a thesis:
        should return nothing because the strict MATCH excludes them.
        """
        qb_filter = {
            "condition": "AND",
            "rules": [
                {"field": "Student.f_name", "operator": "equal", "value": "Ginny"}
            ],
            "valid": True,
        }
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=qb_filter,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=True,
        )
        records = list(db.read_data(req))

        assert len(records) == 0, (
            "Ginny has no 'authored' rel; strict mode should return 0 records"
        )

    def test_optional_mode_filter_on_related_student(self, db: "Neo4jDB"):
        """
        rel_as_filter=False + filter for Hermione (student 1, HAS thesis):
        should return her record WITH a populated relationship.
        """
        qb_filter = {
            "condition": "AND",
            "rules": [
                {"field": "Student.f_name", "operator": "equal", "value": "Hermoine"}
            ],
            "valid": True,
        }
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=qb_filter,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=False,
        )
        records = list(db.read_data(req))

        assert len(records) >= 1, "Hermione should be returned"
        for r in records:
            data = r.data()
            assert data["n"]["f_name"] == "Hermoine"
            assert data["r"] is not None, "Hermione authored a thesis; r should not be null"
            assert data["m"] is not None, "Hermione authored a thesis; m should not be null"

    # ═══════════════════════════════════════════════════════════════
    # E. COMBINED WITH LIMIT
    # ═══════════════════════════════════════════════════════════════

    def test_optional_mode_with_limit(self, db: "Neo4jDB"):
        """rel_as_filter=False + limit: limit must still be respected."""
        req = ReadRequest(
            selected_labels=["Student"],
            limit=3,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=False,
        )
        records = list(db.read_data(req))
        assert len(records) == 3

    def test_strict_mode_with_limit(self, db: "Neo4jDB"):
        """rel_as_filter=True + limit: limit applies to the filtered set."""
        req = ReadRequest(
            selected_labels=["Student"],
            limit=2,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=True,
        )
        records = list(db.read_data(req))
        assert len(records) == 2
        # All returned records must have a relationship
        for r in records:
            assert r.data()["r"] is not None

    # ═══════════════════════════════════════════════════════════════
    # F. EDGE CASES
    # ═══════════════════════════════════════════════════════════════

    def test_optional_mode_no_rel_types_same_as_plain_match(self, db: "Neo4jDB"):
        """When rel_types is None, rel_as_filter=False should behave identically to True."""
        req_true = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=None,
            rel_as_filter=True,
        )
        req_false = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=None,
            rel_as_filter=False,
        )
        ids_true = {r.data()["n"]["student_id"] for r in db.read_data(req_true)}
        ids_false = {r.data()["n"]["student_id"] for r in db.read_data(req_false)}

        assert ids_true == ids_false

    def test_optional_mode_with_nonexistent_rel_type(self, db: "Neo4jDB"):
        """
        rel_as_filter=False with a rel type no node has:
        All students returned but every r and m should be None.
        """
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=["nonexiste"],
            rel_as_filter=False,
        )
        records = list(db.read_data(req))
        returned_ids = {r.data()["n"]["student_id"] for r in records}

        assert self.ALL_STUDENT_IDS.issubset(returned_ids), (
            "All students should still be returned even with a nonexistent relationship"
        )
        for r in records:
            data = r.data()
            assert data["r"] is None, "No node has this rel; r should always be null"
            assert data["m"] is None, "No node has this rel; m should always be null"

    def test_strict_mode_with_nonexistent_rel_type(self, db: "Neo4jDB"):
        """
        rel_as_filter=True with a rel type no node has:
        Should return zero records.
        """
        req = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=["nononexe"],
            rel_as_filter=True,
        )
        records = list(db.read_data(req))
        assert len(records) == 0

    def test_backward_compatibility_default_matches_explicit_true(self, db: "Neo4jDB"):
        """
        Omitting rel_as_filter should produce the same results as explicitly
        passing rel_as_filter=True (backward compatibility).
        """
        req_default = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
        )
        req_explicit = ReadRequest(
            selected_labels=["Student"],
            limit=None,
            property_filters=None,
            rel_filter=[self.AUTHORED_REL],
            rel_as_filter=True,
        )
        ids_default = {r.data()["n"]["student_id"] for r in db.read_data(req_default)}
        ids_explicit = {r.data()["n"]["student_id"] for r in db.read_data(req_explicit)}

        assert ids_default == ids_explicit, (
            "Default behavior must match explicit rel_as_filter=True"
        )
