from dataclasses import dataclass
import logging

LOG = "[n4j] label validator"

@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    bad: list[str]

CYPHER_RESERVED_WORDS = {
    "ALL",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "ASCENDING",
    "BY",
    "CONTAINS",
    "CREATE",
    "DELETE",
    "DESC",
    "DESCENDING",
    "DETACH",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "ENDS",
    "EXISTS",
    "FALSE",
    "FILTER",
    "FOREACH",
    "IN",
    "IS",
    "LIMIT",
    "MATCH",
    "MERGE",
    "NOT",
    "NULL",
    "ON",
    "OPTIONAL",
    "OR",
    "ORDER",
    "REMOVE",
    "RETURN",
    "SET",
    "SKIP",
    "STARTS",
    "THEN",
    "TRUE",
    "UNION",
    "UNIQUE",
    "UNWIND",
    "WHEN",
    "WHERE",
    "WITH",
    "XOR"
}

def label_validation(label: str, logger=logging.getLogger(LOG) )-> bool:
    """Checks label for bad characters or cypher reserved words. \n
    Returns False if any of them is detected, true if not."""
    if not label.replace("_", "").isalnum():
        logger.error(f"Invalid label format: {label}")
        return False
    for word in CYPHER_RESERVED_WORDS:
        if word in label:
            return False
    return True

def validate_labels(node_labels, node_filters, rel_types, logger=logging.getLogger(LOG)) -> ValidationResult:
    bad: list[str] = []
    if not node_labels: return ValidationResult(False, ["empty string as labels"])
    label_groups = [node_labels, node_filters, rel_types]
    for group in label_groups:
        if not group:
            continue

        if isinstance(group, str):
            items = [group]
        else:
            try:
                items = list(group)
            except TypeError:
                bad.append(str(group))
                continue

        for item in items:
            if not isinstance(item, str):
                bad.append(repr(item))
                continue
            if not label_validation(item, logger):
                bad.append(item)

    return ValidationResult(ok=not bad, bad=bad)
