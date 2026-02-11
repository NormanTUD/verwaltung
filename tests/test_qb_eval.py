import pytest
from api.qb_helpers import QueryBuilderProcessor, CypherWhereClause


# =============================================================================
# VALID TEST CASES WITH IDs
# =============================================================================

valid_test_cases = [
    # -------------------------------------------------------------------------
    # BASIC EQUALITY OPERATORS
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.student_id", "operator": "equal", "value": 1}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.student_id = $qb_student_student_id_1",
            parameters={"qb_student_student_id_1": 1},
            is_valid=True,
            errors=[]
        ),
        id="equal-int-student_id"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.f_name", "operator": "equal", "value": "Hermoine"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.f_name = $qb_student_f_name_1",
            parameters={"qb_student_f_name_1": "Hermoine"},
            is_valid=True,
            errors=[]
        ),
        id="equal-str-student_name"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.is_published", "operator": "equal", "value": True}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.is_published = $qb_thesis_is_published_1",
            parameters={"qb_thesis_is_published_1": True},
            is_valid=True,
            errors=[]
        ),
        id="equal-bool-true"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.grade", "operator": "equal", "value": 95.5}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.grade = $qb_thesis_grade_1",
            parameters={"qb_thesis_grade_1": 95.5},
            is_valid=True,
            errors=[]
        ),
        id="equal-float-grade"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Seminar.level", "operator": "not_equal", "value": 5}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.level <> $qb_seminar_level_1",
            parameters={"qb_seminar_level_1": 5},
            is_valid=True,
            errors=[]
        ),
        id="not_equal-int-seminar_level"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Teacher.title", "operator": "not_equal", "value": "Professor"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.title <> $qb_teacher_title_1",
            parameters={"qb_teacher_title_1": "Professor"},
            is_valid=True,
            errors=[]
        ),
        id="not_equal-str-teacher_title"
    ),

    # -------------------------------------------------------------------------
    # COMPARISON OPERATORS
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Seminar.level", "operator": "less", "value": 3}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.level < $qb_seminar_level_1",
            parameters={"qb_seminar_level_1": 3},
            is_valid=True,
            errors=[]
        ),
        id="less-int-seminar_level"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.year", "operator": "less", "value": 2000}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.year < $qb_thesis_year_1",
            parameters={"qb_thesis_year_1": 2000},
            is_valid=True,
            errors=[]
        ),
        id="less-int-thesis_year"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.pages", "operator": "less_or_equal", "value": 100}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.pages <= $qb_thesis_pages_1",
            parameters={"qb_thesis_pages_1": 100},
            is_valid=True,
            errors=[]
        ),
        id="less_or_equal-int-pages"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.grade", "operator": "less_or_equal", "value": 90.0}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.grade <= $qb_thesis_grade_1",
            parameters={"qb_thesis_grade_1": 90.0},
            is_valid=True,
            errors=[]
        ),
        id="less_or_equal-float-grade"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Seminar.level", "operator": "greater", "value": 2}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.level > $qb_seminar_level_1",
            parameters={"qb_seminar_level_1": 2},
            is_valid=True,
            errors=[]
        ),
        id="greater-int-seminar_level"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.pages", "operator": "greater", "value": 150}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.pages > $qb_thesis_pages_1",
            parameters={"qb_thesis_pages_1": 150},
            is_valid=True,
            errors=[]
        ),
        id="greater-int-pages"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.grade", "operator": "greater_or_equal", "value": 85.0}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.grade >= $qb_thesis_grade_1",
            parameters={"qb_thesis_grade_1": 85.0},
            is_valid=True,
            errors=[]
        ),
        id="greater_or_equal-float-grade"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.student_id", "operator": "greater_or_equal", "value": 1}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.student_id >= $qb_student_student_id_1",
            parameters={"qb_student_student_id_1": 1},
            is_valid=True,
            errors=[]
        ),
        id="greater_or_equal-int-boundary"
    ),

    # -------------------------------------------------------------------------
    # STRING OPERATORS
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.title", "operator": "contains", "value": "Potions"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.title CONTAINS $qb_str_1",
            parameters={"qb_str_1": "Potions"},
            is_valid=True,
            errors=[]
        ),
        id="contains-simple"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Seminar.name", "operator": "contains", "value": "Dark Arts"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.name CONTAINS $qb_str_1",
            parameters={"qb_str_1": "Dark Arts"},
            is_valid=True,
            errors=[]
        ),
        id="contains-with_spaces"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.title", "operator": "not_contains", "value": "Quidditch"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="NOT (n.title CONTAINS $qb_str_1)",
            parameters={"qb_str_1": "Quidditch"},
            is_valid=True,
            errors=[]
        ),
        id="not_contains-title"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.department", "operator": "not_contains", "value": "History"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="NOT (n.department CONTAINS $qb_str_1)",
            parameters={"qb_str_1": "History"},
            is_valid=True,
            errors=[]
        ),
        id="not_contains-department"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.l_name", "operator": "begins_with", "value": "W"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.l_name STARTS WITH $qb_str_1",
            parameters={"qb_str_1": "W"},
            is_valid=True,
            errors=[]
        ),
        id="begins_with-single_char"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.topic", "operator": "begins_with", "value": "Def"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.topic STARTS WITH $qb_str_1",
            parameters={"qb_str_1": "Def"},
            is_valid=True,
            errors=[]
        ),
        id="begins_with-prefix"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.l_name", "operator": "ends_with", "value": "ley"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.l_name ENDS WITH $qb_str_1",
            parameters={"qb_str_1": "ley"},
            is_valid=True,
            errors=[]
        ),
        id="ends_with-suffix"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.title", "operator": "ends_with", "value": "Guide"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.title ENDS WITH $qb_str_1",
            parameters={"qb_str_1": "Guide"},
            is_valid=True,
            errors=[]
        ),
        id="ends_with-word"
    ),

    # -------------------------------------------------------------------------
    # NULL OPERATORS
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.deleted_at", "operator": "is_null", "value": None}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.deleted_at IS NULL",
            parameters={},
            is_valid=True,
            errors=[]
        ),
        id="is_null-with_value_key"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.middle_name", "operator": "is_null"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.middle_name IS NULL",
            parameters={},
            is_valid=True,
            errors=[]
        ),
        id="is_null-without_value_key"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.submission_date", "operator": "is_not_null", "value": None}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.submission_date IS NOT NULL",
            parameters={},
            is_valid=True,
            errors=[]
        ),
        id="is_not_null-with_value_key"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Teacher.f_name", "operator": "is_not_null"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.f_name IS NOT NULL",
            parameters={},
            is_valid=True,
            errors=[]
        ),
        id="is_not_null-without_value_key"
    ),

    # -------------------------------------------------------------------------
    # IN / NOT IN OPERATORS
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.student_id", "operator": "in", "value": [1, 2, 3]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.student_id IN $qb_in_list_1",
            parameters={"qb_in_list_1": [1, 2, 3]},
            is_valid=True,
            errors=[]
        ),
        id="in-int_list"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.department", "operator": "in", "value": ["Science", "Biology", "Defense"]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.department IN $qb_in_list_1",
            parameters={"qb_in_list_1": ["Science", "Biology", "Defense"]},
            is_valid=True,
            errors=[]
        ),
        id="in-str_list"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Seminar.level", "operator": "in", "value": [5]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.level IN $qb_in_list_1",
            parameters={"qb_in_list_1": [5]},
            is_valid=True,
            errors=[]
        ),
        id="in-single_value_list"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.year", "operator": "not_in", "value": [1998, 1999]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="NOT (n.year IN $qb_in_list_1)",
            parameters={"qb_in_list_1": [1998, 1999]},
            is_valid=True,
            errors=[]
        ),
        id="not_in-int_list"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.topic", "operator": "not_in", "value": ["Economics", "Potions"]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="NOT (n.topic IN $qb_in_list_1)",
            parameters={"qb_in_list_1": ["Economics", "Potions"]},
            is_valid=True,
            errors=[]
        ),
        id="not_in-str_list"
    ),

    # -------------------------------------------------------------------------
    # BETWEEN OPERATORS
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.year", "operator": "between", "value": [1998, 2000]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="(n.year >= $qb_between_min_1 AND n.year <= $qb_between_max_2)",
            parameters={"qb_between_min_1": 1998, "qb_between_max_2": 2000},
            is_valid=True,
            errors=[]
        ),
        id="between-int-year"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.grade", "operator": "between", "value": [85.0, 95.0]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="(n.grade >= $qb_between_min_1 AND n.grade <= $qb_between_max_2)",
            parameters={"qb_between_min_1": 85.0, "qb_between_max_2": 95.0},
            is_valid=True,
            errors=[]
        ),
        id="between-float-grade"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.pages", "operator": "between", "value": [80, 150]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="(n.pages >= $qb_between_min_1 AND n.pages <= $qb_between_max_2)",
            parameters={"qb_between_min_1": 80, "qb_between_max_2": 150},
            is_valid=True,
            errors=[]
        ),
        id="between-int-pages"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Seminar.level", "operator": "not_between", "value": [2, 4]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="NOT (n.level >= $qb_between_min_1 AND n.level <= $qb_between_max_2)",
            parameters={"qb_between_min_1": 2, "qb_between_max_2": 4},
            is_valid=True,
            errors=[]
        ),
        id="not_between-int-level"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.grade", "operator": "not_between", "value": [0.0, 84.9]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="NOT (n.grade >= $qb_between_min_1 AND n.grade <= $qb_between_max_2)",
            parameters={"qb_between_min_1": 0.0, "qb_between_max_2": 84.9},
            is_valid=True,
            errors=[]
        ),
        id="not_between-float-grade"
    ),

    # -------------------------------------------------------------------------
    # AND CONDITIONS - MULTIPLE RULES
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.is_published", "operator": "equal", "value": True},
                {"field": "Thesis.grade", "operator": "greater_or_equal", "value": 90.0}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.is_published = $qb_thesis_is_published_1 AND n.grade >= $qb_thesis_grade_2",
            parameters={"qb_thesis_is_published_1": True, "qb_thesis_grade_2": 90.0},
            is_valid=True,
            errors=[]
        ),
        id="and-two_rules"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.student_id", "operator": "greater", "value": 0},
                {"field": "Student.f_name", "operator": "is_not_null"},
                {"field": "Student.l_name", "operator": "begins_with", "value": "G"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.student_id > $qb_student_student_id_1 AND n.f_name IS NOT NULL AND n.l_name STARTS WITH $qb_str_2",
            parameters={"qb_student_student_id_1": 0, "qb_str_2": "G"},
            is_valid=True,
            errors=[]
        ),
        id="and-three_rules_mixed_ops"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.year", "operator": "equal", "value": 1998},
                {"field": "Thesis.is_published", "operator": "equal", "value": True},
                {"field": "Thesis.pages", "operator": "greater", "value": 100},
                {"field": "Thesis.department", "operator": "not_equal", "value": "History"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.year = $qb_thesis_year_1 AND n.is_published = $qb_thesis_is_published_2 AND n.pages > $qb_thesis_pages_3 AND n.department <> $qb_thesis_department_4",
            parameters={
                "qb_thesis_year_1": 1998,
                "qb_thesis_is_published_2": True,
                "qb_thesis_pages_3": 100,
                "qb_thesis_department_4": "History"
            },
            is_valid=True,
            errors=[]
        ),
        id="and-four_rules_comprehensive"
    ),

    # -------------------------------------------------------------------------
    # OR CONDITIONS - MULTIPLE RULES
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "OR",
            "rules": [
                {"field": "Seminar.level", "operator": "equal", "value": 1},
                {"field": "Seminar.level", "operator": "equal", "value": 5}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.level = $qb_seminar_level_1 OR n.level = $qb_seminar_level_2",
            parameters={"qb_seminar_level_1": 1, "qb_seminar_level_2": 5},
            is_valid=True,
            errors=[]
        ),
        id="or-two_rules_same_field"
    ),
    pytest.param(
        {
            "condition": "OR",
            "rules": [
                {"field": "Thesis.topic", "operator": "equal", "value": "Potions"},
                {"field": "Thesis.topic", "operator": "equal", "value": "Defense"},
                {"field": "Thesis.topic", "operator": "equal", "value": "Herbology"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.topic = $qb_thesis_topic_1 OR n.topic = $qb_thesis_topic_2 OR n.topic = $qb_thesis_topic_3",
            parameters={
                "qb_thesis_topic_1": "Potions",
                "qb_thesis_topic_2": "Defense",
                "qb_thesis_topic_3": "Herbology"
            },
            is_valid=True,
            errors=[]
        ),
        id="or-three_rules_same_field"
    ),
    pytest.param(
        {
            "condition": "OR",
            "rules": [
                {"field": "Student.f_name", "operator": "contains", "value": "Har"},
                {"field": "Student.l_name", "operator": "equal", "value": "Weasley"},
                {"field": "Student.student_id", "operator": "equal", "value": 1}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.f_name CONTAINS $qb_str_1 OR n.l_name = $qb_student_l_name_2 OR n.student_id = $qb_student_student_id_3",
            parameters={
                "qb_str_1": "Har",
                "qb_student_l_name_2": "Weasley",
                "qb_student_student_id_3": 1
            },
            is_valid=True,
            errors=[]
        ),
        id="or-mixed_operators_student_search"
    ),

    # -------------------------------------------------------------------------
    # NESTED GROUPS
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.is_published", "operator": "equal", "value": True},
                {
                    "condition": "OR",
                    "rules": [
                        {"field": "Thesis.topic", "operator": "equal", "value": "Potions"},
                        {"field": "Thesis.topic", "operator": "equal", "value": "Defense"}
                    ]
                }
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.is_published = $qb_thesis_is_published_1 AND (n.topic = $qb_thesis_topic_2 OR n.topic = $qb_thesis_topic_3)",
            parameters={
                "qb_thesis_is_published_1": True,
                "qb_thesis_topic_2": "Potions",
                "qb_thesis_topic_3": "Defense"
            },
            is_valid=True,
            errors=[]
        ),
        id="nested-and_with_or"
    ),
    pytest.param(
        {
            "condition": "OR",
            "rules": [
                {
                    "condition": "AND",
                    "rules": [
                        {"field": "Thesis.year", "operator": "equal", "value": 1998},
                        {"field": "Thesis.is_published", "operator": "equal", "value": True}
                    ]
                },
                {
                    "condition": "AND",
                    "rules": [
                        {"field": "Thesis.year", "operator": "equal", "value": 2000},
                        {"field": "Thesis.grade", "operator": "greater", "value": 99.0}
                    ]
                }
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="(n.year = $qb_thesis_year_1 AND n.is_published = $qb_thesis_is_published_2) OR (n.year = $qb_thesis_year_3 AND n.grade > $qb_thesis_grade_4)",
            parameters={
                "qb_thesis_year_1": 1998,
                "qb_thesis_is_published_2": True,
                "qb_thesis_year_3": 2000,
                "qb_thesis_grade_4": 99.0
            },
            is_valid=True,
            errors=[]
        ),
        id="nested-or_with_two_and_groups"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.submission_date", "operator": "is_not_null"},
                {
                    "condition": "OR",
                    "rules": [
                        {
                            "condition": "AND",
                            "rules": [
                                {"field": "Thesis.department", "operator": "equal", "value": "Science"},
                                {"field": "Thesis.grade", "operator": "greater_or_equal", "value": 95.0}
                            ]
                        },
                        {"field": "Thesis.is_published", "operator": "equal", "value": True}
                    ]
                }
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.submission_date IS NOT NULL AND ((n.department = $qb_thesis_department_1 AND n.grade >= $qb_thesis_grade_2) OR n.is_published = $qb_thesis_is_published_3)",
            parameters={
                "qb_thesis_department_1": "Science",
                "qb_thesis_grade_2": 95.0,
                "qb_thesis_is_published_3": True
            },
            is_valid=True,
            errors=[]
        ),
        id="nested-triple_depth"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.year", "operator": "between", "value": [1998, 2000]},
                {
                    "condition": "OR",
                    "rules": [
                        {
                            "condition": "AND",
                            "rules": [
                                {"field": "Thesis.department", "operator": "in", "value": ["Science", "Biology"]},
                                {"field": "Thesis.pages", "operator": "greater", "value": 100}
                            ]
                        },
                        {
                            "condition": "AND",
                            "rules": [
                                {"field": "Thesis.department", "operator": "equal", "value": "Defense"},
                                {"field": "Thesis.is_published", "operator": "equal", "value": True}
                            ]
                        }
                    ]
                }
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="(n.year >= $qb_between_min_1 AND n.year <= $qb_between_max_2) AND ((n.department IN $qb_in_list_3 AND n.pages > $qb_thesis_pages_4) OR (n.department = $qb_thesis_department_5 AND n.is_published = $qb_thesis_is_published_6))",
            parameters={
                "qb_between_min_1": 1998,
                "qb_between_max_2": 2000,
                "qb_in_list_3": ["Science", "Biology"],
                "qb_thesis_pages_4": 100,
                "qb_thesis_department_5": "Defense",
                "qb_thesis_is_published_6": True
            },
            is_valid=True,
            errors=[]
        ),
        id="nested-complex_multi_branch"
    ),

    # -------------------------------------------------------------------------
    # REAL-WORLD HOGWARTS SCENARIOS
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.is_published", "operator": "equal", "value": True},
                {"field": "Thesis.grade", "operator": "greater_or_equal", "value": 90.0},
                {"field": "Thesis.pages", "operator": "greater", "value": 100},
                {"field": "Thesis.title", "operator": "not_contains", "value": "Quidditch"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.is_published = $qb_thesis_is_published_1 AND n.grade >= $qb_thesis_grade_2 AND n.pages > $qb_thesis_pages_3 AND NOT (n.title CONTAINS $qb_str_4)",
            parameters={
                "qb_thesis_is_published_1": True,
                "qb_thesis_grade_2": 90.0,
                "qb_thesis_pages_3": 100,
                "qb_str_4": "Quidditch"
            },
            is_valid=True,
            errors=[]
        ),
        id="scenario-high_achieving_theses"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.l_name", "operator": "equal", "value": "Weasley"},
                {
                    "condition": "OR",
                    "rules": [
                        {"field": "Student.f_name", "operator": "equal", "value": "Ron"},
                        {"field": "Student.f_name", "operator": "equal", "value": "Ginny"}
                    ]
                }
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.l_name = $qb_student_l_name_1 AND (n.f_name = $qb_student_f_name_2 OR n.f_name = $qb_student_f_name_3)",
            parameters={
                "qb_student_l_name_1": "Weasley",
                "qb_student_f_name_2": "Ron",
                "qb_student_f_name_3": "Ginny"
            },
            is_valid=True,
            errors=[]
        ),
        id="scenario-weasley_siblings"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Seminar.level", "operator": "not_between", "value": [1, 2]},
                {"field": "Seminar.name", "operator": "is_not_null"}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="NOT (n.level >= $qb_between_min_1 AND n.level <= $qb_between_max_2) AND n.name IS NOT NULL",
            parameters={"qb_between_min_1": 1, "qb_between_max_2": 2},
            is_valid=True,
            errors=[]
        ),
        id="scenario-advanced_seminars"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.department", "operator": "equal", "value": "Biology"},
                {
                    "condition": "OR",
                    "rules": [
                        {"field": "Thesis.topic", "operator": "contains", "value": "Flora"},
                        {"field": "Thesis.topic", "operator": "contains", "value": "Creatures"},
                        {"field": "Thesis.title", "operator": "begins_with", "value": "Flora"}
                    ]
                },
                {"field": "Thesis.grade", "operator": "greater", "value": 80.0}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.department = $qb_thesis_department_1 AND (n.topic CONTAINS $qb_str_2 OR n.topic CONTAINS $qb_str_3 OR n.title STARTS WITH $qb_str_4) AND n.grade > $qb_thesis_grade_5",
            parameters={
                "qb_thesis_department_1": "Biology",
                "qb_str_2": "Flora",
                "qb_str_3": "Creatures",
                "qb_str_4": "Flora",
                "qb_thesis_grade_5": 80.0
            },
            is_valid=True,
            errors=[]
        ),
        id="scenario-biology_dept_search"
    ),

    # -------------------------------------------------------------------------
    # EDGE CASES
    # -------------------------------------------------------------------------
    pytest.param(
        {
            "condition": "AND",
            "rules": [],
            "valid": True
        },
        CypherWhereClause(
            clause="",
            parameters={},
            is_valid=True,
            errors=[]
        ),
        id="edge-empty_rules_list"
    ),
    pytest.param(
        {
            "condition": "OR",
            "rules": [
                {"field": "Seminar.level", "operator": "equal", "value": 3}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.level = $qb_seminar_level_1",
            parameters={"qb_seminar_level_1": 3},
            is_valid=True,
            errors=[]
        ),
        id="edge-single_rule_with_or"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.grade", "operator": "greater_or_equal", "value": 85.0},
                {"field": "Thesis.grade", "operator": "less_or_equal", "value": 95.0}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.grade >= $qb_thesis_grade_1 AND n.grade <= $qb_thesis_grade_2",
            parameters={"qb_thesis_grade_1": 85.0, "qb_thesis_grade_2": 95.0},
            is_valid=True,
            errors=[]
        ),
        id="edge-duplicate_field_unique_params"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.middle_name", "operator": "equal", "value": ""}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.middle_name = $qb_student_middle_name_1",
            parameters={"qb_student_middle_name_1": ""},
            is_valid=True,
            errors=[]
        ),
        id="edge-empty_string_value"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.pages", "operator": "equal", "value": 0}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.pages = $qb_thesis_pages_1",
            parameters={"qb_thesis_pages_1": 0},
            is_valid=True,
            errors=[]
        ),
        id="edge-zero_value"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "level", "operator": "equal", "value": 3}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.level = $qb_level_1",
            parameters={"qb_level_1": 3},
            is_valid=True,
            errors=[]
        ),
        id="edge-simple_field_no_dot"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.is_published", "operator": "equal", "value": False}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.is_published = $qb_thesis_is_published_1",
            parameters={"qb_thesis_is_published_1": False},
            is_valid=True,
            errors=[]
        ),
        id="edge-false_boolean"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Account.balance", "operator": "greater", "value": -100}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.balance > $qb_account_balance_1",
            parameters={"qb_account_balance_1": -100},
            is_valid=True,
            errors=[]
        ),
        id="edge-negative_number"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.student_id", "operator": "in", "value": [1, 2, 3, 4, 5, 6, 7, 8]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="n.student_id IN $qb_in_list_1",
            parameters={"qb_in_list_1": [1, 2, 3, 4, 5, 6, 7, 8]},
            is_valid=True,
            errors=[]
        ),
        id="edge-large_in_list"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Seminar.level", "operator": "between", "value": [3, 3]}
            ],
            "valid": True
        },
        CypherWhereClause(
            clause="(n.level >= $qb_between_min_1 AND n.level <= $qb_between_max_2)",
            parameters={"qb_between_min_1": 3, "qb_between_max_2": 3},
            is_valid=True,
            errors=[]
        ),
        id="edge-between_same_min_max"
    ),
]


# =============================================================================
# INVALID TEST CASES WITH IDs
# =============================================================================

invalid_test_cases = [
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.f_name", "operator": "equal", "value": "Harry"}
            ],
            "valid": False
        },
        "invalid state",
        id="invalid-valid_flag_false"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Student.f_name", "operator": "regex_match", "value": ".*Harry.*"}
            ],
            "valid": True
        },
        "Unsupported operator",
        id="invalid-unsupported_operator"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.year", "operator": "between", "value": [1998]}
            ],
            "valid": True
        },
        "BETWEEN requires exactly 2 values",
        id="invalid-between_single_value"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Thesis.grade", "operator": "between", "value": [80, 90, 100]}
            ],
            "valid": True
        },
        "BETWEEN requires exactly 2 values",
        id="invalid-between_three_values"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "", "operator": "equal", "value": "test"}
            ],
            "valid": True
        },
        "empty field",
        id="invalid-empty_field_name"
    ),
    pytest.param(
        {
            "condition": "AND",
            "rules": [
                {"field": "Schema.Table.Column", "operator": "equal", "value": "test"}
            ],
            "valid": True
        },
        "Invalid field format",
        id="invalid-triple_dot_notation"
    ),
]


# =============================================================================
# TEST CLASS
# =============================================================================

class TestQueryBuilderProcessor:
    """Test suite for QueryBuilderProcessor using parametrized tests."""

    @pytest.fixture
    def processor(self) -> QueryBuilderProcessor:
        """Fresh processor instance for each test."""
        return QueryBuilderProcessor(node_alias="n")

    # -------------------------------------------------------------------------
    # VALID INPUT TESTS
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("qb_input,expected", valid_test_cases)
    def test_valid_qb_processing(
        self,
        processor: QueryBuilderProcessor,
        qb_input: dict,
        expected: CypherWhereClause
    ):
        """Test valid QueryBuilder inputs produce expected CypherWhereClause."""
        result = processor.process(qb_input)

        assert result.is_valid, f"Expected valid result, got errors: {result.errors}"
        assert result.clause == expected.clause, (
            f"Clause mismatch:\n"
            f"  Expected: {expected.clause}\n"
            f"  Actual:   {result.clause}"
        )
        assert result.parameters == expected.parameters, (
            f"Parameters mismatch:\n"
            f"  Expected: {expected.parameters}\n"
            f"  Actual:   {result.parameters}"
        )

    # -------------------------------------------------------------------------
    # INVALID INPUT TESTS
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("qb_input,expected_error", invalid_test_cases)
    def test_invalid_qb_processing(
        self,
        processor: QueryBuilderProcessor,
        qb_input: dict,
        expected_error: str
    ):
        """Test invalid QueryBuilder inputs are properly rejected."""
        result = processor.process(qb_input)

        assert not result.is_valid, (
            f"Expected invalid result, but got valid with clause: {result.clause}"
        )
        assert any(expected_error.lower() in err.lower() for err in result.errors), (
            f"Expected error containing '{expected_error}'\n"
            f"  Actual errors: {result.errors}"
        )

    # -------------------------------------------------------------------------
    # STATE MANAGEMENT TESTS
    # -------------------------------------------------------------------------

    def test_processor_state_reset_between_calls(self, processor: QueryBuilderProcessor):
        """Ensure processor resets internal state between sequential calls."""
        qb1 = {
            "condition": "AND",
            "rules": [{"field": "Thesis.year", "operator": "equal", "value": 1998}],
            "valid": True
        }
        qb2 = {
            "condition": "AND",
            "rules": [{"field": "Student.student_id", "operator": "equal", "value": 1}],
            "valid": True
        }

        result1 = processor.process(qb1)
        result2 = processor.process(qb2)

        # First call should have thesis param
        assert "qb_thesis_year_1" in result1.parameters

        # Second call should start fresh - param counter resets to 1
        assert "qb_student_student_id_1" in result2.parameters
        assert "qb_thesis_year_1" not in result2.parameters
        assert len(result2.parameters) == 1

    def test_processor_errors_reset_between_calls(self, processor: QueryBuilderProcessor):
        """Ensure processor errors don't persist between calls."""
        invalid_qb = {
            "condition": "AND",
            "rules": [{"field": "X.y", "operator": "unknown_op", "value": 1}],
            "valid": True
        }
        valid_qb = {
            "condition": "AND",
            "rules": [{"field": "X.y", "operator": "equal", "value": 1}],
            "valid": True
        }

        result1 = processor.process(invalid_qb)
        result2 = processor.process(valid_qb)

        assert not result1.is_valid
        assert result1.errors  # Should have errors

        assert result2.is_valid
        assert not result2.errors  # Errors should be cleared

    # -------------------------------------------------------------------------
    # CUSTOM NODE ALIAS TEST
    # -------------------------------------------------------------------------

    def test_custom_node_alias(self):
        """Test processor respects custom node alias configuration."""
        processor = QueryBuilderProcessor(node_alias="entity")
        qb_input = {
            "condition": "AND",
            "rules": [{"field": "User.name", "operator": "equal", "value": "test"}],
            "valid": True
        }

        result = processor.process(qb_input)

        assert result.is_valid
        assert "entity.name" in result.clause
        assert "n.name" not in result.clause
