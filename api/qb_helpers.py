from __future__ import annotations
from typing import TypedDict, Literal, Any
from dataclasses import dataclass

from api.neo4j_interface_helpers import label_validation


class QBRule(TypedDict, total=False):
    """Single rule from jQuery QueryBuilder."""
    id: str
    field: str
    type: Literal["string", "integer", "double", "date", "time", "datetime", "boolean"]
    input: str
    operator: str
    value: str | int | float | bool | list | None



class QBGroup(TypedDict, total=False):
    """Recursive group structure from jQuery QueryBuilder."""
    condition: Literal["AND", "OR"]
    rules: list[QBGroup | QBRule]
    valid: bool


@dataclass
class CypherWhereClause:
    """Result of processing a QueryBuilder JSON."""
    clause: str
    parameters: dict[str, Any]
    is_valid: bool
    errors: list[str]


# Cypher operator mapping
OPERATOR_MAP: dict[str, str] = {
    "equal": "=",
    "not_equal": "<>",
    "less": "<",
    "less_or_equal": "<=",
    "greater": ">",
    "greater_or_equal": ">=",
    "contains": "CONTAINS",
    "not_contains": "CONTAINS",  # Handled with NOT wrapper
    "begins_with": "STARTS WITH",
    "ends_with": "ENDS WITH",
    "is_null": "IS NULL",
    "is_not_null": "IS NOT NULL",
    "in": "IN",
    "not_in": "IN",  # Handled with NOT wrapper
}


class QueryBuilderProcessor:
    """
    Processes jQuery QueryBuilder JSON into parameterized Cypher WHERE clauses.

    Usage:
        processor = QueryBuilderProcessor(node_alias="n")
        result = processor.process(qb_json)
        if result.is_valid:
            where_clause = result.clause
            params = result.parameters
    """

    def __init__(self, node_alias: str = "n"):
        self.node_alias = node_alias
        self._param_counter = 0
        self._parameters: dict[str, Any] = {}
        self._errors: list[str] = []

    def _generate_param_name(self, field: str) -> str:
        """Generate unique parameter name to avoid collisions."""
        self._param_counter += 1
        # Sanitize field name for param: Seminar.level -> seminar_level
        safe_field = field.replace(".", "_").lower()
        return f"qb_{safe_field}_{self._param_counter}"

    def _validate_field(self, field: str) -> tuple[bool, str]:
        """
        Validate and extract property name from QueryBuilder field.
        Expects format: 'NodeLabel.property' or just 'property'
        """
        if not field:
            return False, "empty field"

        # Handle 'Label.property' format - extract just the property
        parts = field.split(".")
        if len(parts) == 2:
            property_name = parts[1]
        elif len(parts) == 1:
            property_name = parts[0]
        else:
            self._errors.append(f"Invalid field format: {field}")
            return False, ""

        # Reuse existing validation
        if not label_validation(property_name):
            self._errors.append(f"Invalid property name: {property_name}")
            return False, ""

        return True, property_name

    def _process_rule(self, rule: QBRule) -> str | None:
        """Process a single QueryBuilder rule into a Cypher condition."""
        field = rule.get("field", "")
        operator = rule.get("operator", "")
        value = rule.get("value")

        # Validate field
        is_valid, property_name = self._validate_field(field)
        if not is_valid:
            self._errors.append(f"Unsupported operator: {property_name}")
            return None

        # Validate operator
        if operator not in OPERATOR_MAP and operator not in ("between", "not_between"):
            self._errors.append(f"Unsupported operator: {operator}")
            return None

        node_prop = f"{self.node_alias}.{property_name}"

        # Handle null checks (no parameter needed)
        if operator == "is_null":
            return f"{node_prop} IS NULL"
        if operator == "is_not_null":
            return f"{node_prop} IS NOT NULL"

        # Handle BETWEEN operators
        if operator in ("between", "not_between"):
            return self._process_between(node_prop, operator, value)

        # Handle IN operators
        if operator in ("in", "not_in"):
            return self._process_in(node_prop, operator, value)

        # Handle string operators
        if operator in ("contains", "not_contains", "begins_with", "ends_with"):
            return self._process_string_operator(node_prop, operator, value)

        # Handle standard comparison operators
        param_name = self._generate_param_name(field)
        self._parameters[param_name] = value
        cypher_op = OPERATOR_MAP[operator]

        return f"{node_prop} {cypher_op} ${param_name}"

    def _process_between(self, node_prop: str, operator: str, value: Any) -> str | None:
        """Process BETWEEN operator with two-value range."""
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            self._errors.append(f"BETWEEN requires exactly 2 values, got: {value}")
            return None

        param_min = self._generate_param_name("between_min")
        param_max = self._generate_param_name("between_max")
        self._parameters[param_min] = value[0]
        self._parameters[param_max] = value[1]

        condition = f"({node_prop} >= ${param_min} AND {node_prop} <= ${param_max})"

        if operator == "not_between":
            return f"NOT {condition}"
        return condition

    def _process_in(self, node_prop: str, operator: str, value: Any) -> str | None:
        """Process IN/NOT IN operators with list values."""
        if not isinstance(value, (list, tuple)):
            value = [value]

        param_name = self._generate_param_name("in_list")
        self._parameters[param_name] = list(value)

        condition = f"{node_prop} IN ${param_name}"

        if operator == "not_in":
            return f"NOT ({condition})"
        return condition

    def _process_string_operator(
        self, node_prop: str, operator: str, value: Any
    ) -> str | None:
        """Process string-specific operators (CONTAINS, STARTS WITH, ENDS WITH)."""
        if not isinstance(value, str):
            self._errors.append(f"String operator '{operator}' requires string value")
            return None

        param_name = self._generate_param_name("str")
        self._parameters[param_name] = value
        cypher_op = OPERATOR_MAP[operator]

        condition = f"{node_prop} {cypher_op} ${param_name}"

        if operator == "not_contains":
            return f"NOT ({condition})"
        return condition

    def _process_group(self, group: QBGroup) -> str | None:
        """Recursively process a QueryBuilder group (AND/OR conditions)."""
        condition = group.get("condition", "AND").upper()
        rules = group.get("rules", [])

        if condition not in ("AND", "OR"):
            self._errors.append(f"Invalid condition type: {condition}")
            return None

        if not rules:
            return None

        clauses: list[str] = []

        for rule in rules:
            # Check if this is a nested group or a rule
            if "condition" in rule and "rules" in rule:
                # It's a nested group - recurse
                nested_clause = self._process_group(rule)  # type: ignore
                if nested_clause:
                    clauses.append(f"({nested_clause})")
            else:
                # It's a rule
                rule_clause = self._process_rule(rule)  # type: ignore
                if rule_clause:
                    clauses.append(rule_clause)

        if not clauses:
            return None

        joiner = f" {condition} "
        return joiner.join(clauses)

    def process(self, qb_json: dict) -> CypherWhereClause:
        """
        Main entry point: Process jQuery QueryBuilder JSON into Cypher WHERE clause.

        Args:
            qb_json: The QueryBuilder JSON output from getRules()

        Returns:
            CypherWhereClause with the generated clause, parameters, and validation status
        """
        # Reset state for new processing
        self._param_counter = 0
        self._parameters = {}
        self._errors = []

        # Check validity flag from QueryBuilder
        if not qb_json.get("valid", False):
            return CypherWhereClause(
                clause="",
                parameters={},
                is_valid=False,
                errors=["QueryBuilder reported invalid state"]
            )

        # Process the root group
        clause = self._process_group(qb_json)

        if self._errors:
            return CypherWhereClause(
                clause="",
                parameters={},
                is_valid=False,
                errors=self._errors
            )

        return CypherWhereClause(
            clause=clause or "",
            parameters=self._parameters.copy(),
            is_valid=True,
            errors=[]
        )

def old_where_clause(node_filters: dict):
    where_clauses: list[str] = []
    parameters: dict[str, Any] = {}

    for prop, value in node_filters.items():
        param_name = f"prop_{prop}"
        where_clauses.append(f"n.{prop} = ${param_name}")
        parameters[param_name] = value

    where_clause = ""
    if where_clauses:
        where_clause = " WHERE " + " AND ".join(where_clauses)

    return where_clause, parameters
