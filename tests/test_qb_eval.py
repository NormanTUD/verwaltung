import pytest
from api.qb_helpers import QueryBuilderProcessor, CypherWhereClause

test_data: list[tuple[dict, CypherWhereClause]] = [
        # =====================================================================
        # BASIC EQUALITY OPERATORS
        # =====================================================================

        # 1. Simple integer equality - Student ID
        (
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
            )
        ),

        # 2. String equality - Student first name
        (
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
            )
        ),

        # 3. Boolean equality - Thesis is_published
        (
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
            )
        ),

        # 4. Float equality - Thesis grade
        (
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
            )
        ),

        # 5. Not equal - Seminar level
        (
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
            )
        ),

        # 6. Not equal string - Teacher title
        (
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
            )
        ),

        # =====================================================================
        # COMPARISON OPERATORS
        # =====================================================================

        # 7. Less than - Seminar level
        (
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
            )
        ),

        # 8. Less than - Thesis year
        (
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
            )
        ),

        # 9. Less than or equal - Thesis pages
        (
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
            )
        ),

        # 10. Less than or equal - Grade (float)
        (
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
            )
        ),

        # 11. Greater than - Seminar level
        (
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
            )
        ),

        # 12. Greater than - Thesis pages
        (
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
            )
        ),

        # 13. Greater than or equal - Thesis grade
        (
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
            )
        ),

        # 14. Greater than or equal - Student ID (boundary)
        (
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
            )
        ),

        # =====================================================================
        # STRING OPERATORS
        # =====================================================================

        # 15. Contains - Thesis title
        (
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
            )
        ),

        # 16. Contains - Seminar name with spaces
        (
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
            )
        ),

        # 17. Not contains - Thesis title
        (
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
            )
        ),

        # 18. Not contains - Department
        (
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
            )
        ),

        # 19. Begins with - Student last name
        (
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
            )
        ),

        # 20. Begins with - Thesis topic
        (
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
            )
        ),

        # 21. Ends with - Student last name
        (
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
            )
        ),

        # 22. Ends with - Thesis title
        (
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
            )
        ),

        # =====================================================================
        # NULL OPERATORS
        # =====================================================================

        # 23. Is null - with value key
        (
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
            )
        ),

        # 24. Is null - without value key
        (
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
            )
        ),

        # 25. Is not null - submission_date
        (
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
            )
        ),

        # 26. Is not null - without value key
        (
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
            )
        ),

        # =====================================================================
        # IN / NOT IN OPERATORS
        # =====================================================================

        # 27. In list of integers - Student IDs
        (
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
            )
        ),

        # 28. In list of strings - Departments
        (
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
            )
        ),

        # 29. In single value list (edge case)
        (
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
            )
        ),

        # 30. Not in list of integers - Thesis years
        (
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
            )
        ),

        # 31. Not in list of strings - Topics
        (
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
            )
        ),

        # =====================================================================
        # BETWEEN OPERATORS
        # =====================================================================

        # 32. Between integers - Thesis year
        (
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
            )
        ),

        # 33. Between floats - Thesis grade
        (
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
            )
        ),

        # 34. Between - Pages
        (
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
            )
        ),

        # 35. Not between integers - Seminar level
        (
            {
                "condition": "AND",
                "rules": [
                    {"field": "Seminar.level", "operator": "not_between", "value": [2, 4]}
                ],
                "valid": True
            },
            CypherWhereClause(
                clause='NOT (n.level >= $qb_between_min_1 AND n.level <= $qb_between_max_2)',
                parameters={"qb_between_min_1": 2, "qb_between_max_2": 4},
                is_valid=True,
                errors=[]
            )
        ),


        # 36. Not between floats - Thesis grade
        (
            {
                "condition": "AND",
                "rules": [
                    {"field": "Thesis.grade", "operator": "not_between", "value": [0.0, 84.9]}
                ],
                "valid": True
            },
            CypherWhereClause(
                clause='NOT (n.grade >= $qb_between_min_1 AND n.grade <= $qb_between_max_2)',
                parameters={"qb_between_min_1": 0.0, "qb_between_max_2": 84.9},
                is_valid=True,
                errors=[]
            )
        ),

        # =====================================================================
        # AND CONDITIONS - MULTIPLE RULES
        # =====================================================================

        # 37. Two AND rules - same entity
        (
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
            )
        ),

        # 38. Three AND rules - Student filter
        (
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
            )
        ),

        # 39. Four AND rules - Thesis comprehensive filter
        (
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
            )
        ),

        # =====================================================================
        # OR CONDITIONS - MULTIPLE RULES
        # =====================================================================

        # 40. Two OR rules - Seminar levels
        (
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
            )
        ),

        # 41. Three OR rules - Mixed operators
        (
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
            )
        ),

        # 42. OR with mixed operators - Student search
        (
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
            )
        ),

        # =====================================================================
        # NESTED GROUPS
        # =====================================================================

        # 43. AND with nested OR - Published thesis by topic
        (
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
            )
        ),

        # 44. OR with nested AND - Find specific theses
        (
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
            )
        ),

        # 45. Triple nesting - Complex thesis filter
        (
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
            )
        ),

        # 46. Deeply nested with multiple branches
        (
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
            )
        ),

        # =====================================================================
        # REAL-WORLD HOGWARTS SCENARIOS
        # =====================================================================

        # 47. Find high-achieving published theses
        (
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
            )
        ),

        # 48. Find Weasley students
        (
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
            )
        ),

        # 49. Seminars not beginner level
        (
            {
                "condition": "AND",
                "rules": [
                    {"field": "Seminar.level", "operator": "not_between", "value": [1, 2]},
                    {"field": "Seminar.name", "operator": "is_not_null"}
                ],
                "valid": True
            },
            CypherWhereClause(
                clause='NOT (n.level >= $qb_between_min_1 AND n.level <= $qb_between_max_2) AND n.name IS NOT NULL',
                parameters={"qb_between_min_1": 1, "qb_between_max_2": 2},
                is_valid=True,
                errors=[]
            )
        ),

        # 50. Biology department thesis search
        (
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
            )
        ),

        # =====================================================================
        # EDGE CASES
        # =====================================================================

        # 51. Empty rules list
        (
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
            )
        ),

        # 52. Single rule with OR condition (no join needed)
        (
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
            )
        ),

        # 53. Duplicate field names - should get unique params
        (
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
            )
        ),

        # 54. Empty string value
        (
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
            )
        ),

        # 55. Zero value
        (
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
            )
        ),

        # 56. Simple field format (no dot notation)
        (
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
            )
        ),

        # 57. False boolean value
        (
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
            )
        ),

        # 58. Negative number comparison
        (
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
            )
        ),

        # 59. Large IN list
        (
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
            )
        ),

        # 60. Between with same min and max
        (
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
            )
        ),
    ]

    # =========================================================================
    # INVALID INPUT TEST DATA (separate list)
    # =========================================================================

invalid_test_data: list[tuple[dict, str]] = [
        # Invalid: valid=False flag
        (
            {
                "condition": "AND",
                "rules": [
                    {"field": "Student.f_name", "operator": "equal", "value": "Harry"}
                ],
                "valid": False
            },
            "invalid state"
        ),

        # Invalid: unsupported operator
        (
            {
                "condition": "AND",
                "rules": [
                    {"field": "Student.f_name", "operator": "regex_match", "value": ".*Harry.*"}
                ],
                "valid": True
            },
            "Unsupported operator"
        ),

        # Invalid: between with single value
        (
            {
                "condition": "AND",
                "rules": [
                    {"field": "Thesis.year", "operator": "between", "value": [1998]}
                ],
                "valid": True
            },
            "BETWEEN requires exactly 2 values"
        ),

        # Invalid: between with three values
        (
            {
                "condition": "AND",
                "rules": [
                    {"field": "Thesis.grade", "operator": "between", "value": [80, 90, 100]}
                ],
                "valid": True
            },
            "BETWEEN requires exactly 2 values"
        ),

        # Invalid: empty field name
        (
            {
                "condition": "AND",
                "rules": [
                    {"field": "", "operator": "equal", "value": "test"}
                ],
                "valid": True
            },
            "Invalid"
        ),

        # Invalid: triple dot notation in field
        (
            {
                "condition": "AND",
                "rules": [
                    {"field": "Schema.Table.Column", "operator": "equal", "value": "test"}
                ],
                "valid": True
            },
            "Invalid field format"
        ),
    ]

class TestQbEval:
    qb_processor = QueryBuilderProcessor()
    test_data: list[tuple[dict, CypherWhereClause]] = test_data

    def test_valid_qb_processing(self):
        """Test all valid QueryBuilder inputs produce expected CypherWhereClause."""
        for idx, (qb_input, expected) in enumerate(TestQbEval.test_data):
            qb_to_result(qb_input, expected, idx)

    def test_invalid_qb_processing(self):
        """Test that invalid QueryBuilder inputs are properly rejected."""
        for idx, (qb_input, expected_error) in enumerate(invalid_test_data):
            result = TestQbEval.qb_processor.process(qb_input)
            assert not result.is_valid, (
                f"Invalid test case {idx + 1} should have failed but didn't"
            )


    def test_processor_state_reset(self):
        """Ensure processor resets state between sequential calls."""
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

        result1 = TestQbEval.qb_processor.process(qb1)
        result2 = TestQbEval.qb_processor.process(qb2)

        # Second call should have param counter starting fresh at 1
        assert "qb_student_student_id_1" in result2.parameters
        assert "qb_thesis_year_1" not in result2.parameters
        assert len(result2.parameters) == 1

def qb_to_result(qb: dict, expected: CypherWhereClause, idx:int):
    """Compare QueryBuilder processing result against expected CypherWhereClause."""
    result = TestQbEval.qb_processor.process(qb)
    error = f"\n [qb_eval_test {idx}]"
    assert result.is_valid == expected.is_valid, error + "Result is not valid"
    assert result.clause == expected.clause, error + f"\n    Clause is not as expected \n        {result.clause=}\n        {expected.clause=} \n"
    assert result.parameters == expected.parameters, error + f"Parameters are not as expected\n    {result.parameters=}\n    {expected.parameters=} \n"
