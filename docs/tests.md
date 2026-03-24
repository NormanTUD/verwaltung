# Test skip/xfail documentation

## test_app.TestNeo4jApp
### test_get_data_as_table_basic_positive_single_person
Here there seems to be a confusion with the way that `api.read_as_table` is working,
this tests expects a chain person-ort-stadt to be displayed on a single line.

The here tested behavior is the **desired behavior** but the route does not serve it.

### test_operator_not_begins_with / test_operator_not_ends_with
These are not implemented in `api.qb_helpers`.

### .test_get_data_as_table_filter_labels_behavior/test_get_data_as_table_filter_labels_excludes_nodes
This tests for a deprecated query parameter: `filterLabels`.

### .test_get_data_as_table_no_properties

### Import and Assert 200 Family
The following list of tests shall be replaced with the parametrized: `tests/test_app.py:4785` - but this is not working as of now, it probably exposed a backend issue.

```
test_app.py:2367
test_app.py:2534
test_app.py:2490
test_app.py:2595
test_app.py:2513
test_app.py:2574
test_app.py:2557
test_app.py:2332
test_app.py:2439
test_app.py:2993
test_app.py:2895
test_app.py:2955
test_app.py:2454
test_app.py:2863
test_app.py:2816
test_app.py:2781
test_app.py:2702
test_app.py:2747
test_app.py:2294
test_app.py:2276
test_app.py:2417
test_app.py:2469
test_app.py:2347
test_app.py:2260
test_app.py:2314
test_app.py:2402
test_app.py:2387
test_app.py:2665
test_app.py:2616
test_app.py:2640
test_app.py:280
test_app.py:307
```

## test_Neo4j_Interface_read
### not begins with / in-with-propertylist / is_empty_filter / not_begins_with
Unsupported qb-expressions in `api.qb_helpers`


