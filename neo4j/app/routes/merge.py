from flask import Blueprint, render_template, request, current_app, jsonify

bp = Blueprint("merge", __name__, template_folder="../../templates", static_folder="../../static")

def canonical(s: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in (s or "").strip())

@bp.route("/", methods=["GET"])
def index():
    # Suggest column name merges by canonicalization and by similarity of values
    neo = current_app.neo
    # get properties used in DB: sample some nodes and merge property names
    sample = neo.run_cypher("MATCH (n) RETURN keys(n) as keys LIMIT 200")
    prop_counter = {}
    for r in sample:
        for k in r.get("keys", []):
            prop_counter.setdefault(k, 0)
            prop_counter[k] += 1
    # group by canonical form
    groups = {}
    for k, cnt in prop_counter.items():
        can = canonical(k)
        groups.setdefault(can, []).append({"name": k, "count": cnt})
    # only show groups with >1 suggestion
    suggestions = {can: lst for can, lst in groups.items() if len(lst) > 1}
    return render_template("merge.html", suggestions=suggestions)

@bp.route("/merge_props", methods=["POST"])
def merge_props():
    """
    expected JSON:
    {
        "label": "Person",           # optional; blank means across labels
        "props": ["telnr","telefon","telefonnummer"],
        "strategy": "overwrite"     # or "array", "prefix:<str>", "suffix:<str>", "sum"
    }
    """
    data = request.get_json() or {}
    props = data.get("props", [])
    strategy = data.get("strategy", "overwrite")
    label = data.get("label", None)
    neo = current_app.neo

    if not props or len(props) < 2:
        return {"error": "need at least two props"}, 400

    # build cypher to merge properties into main_prop = props[0]
    main = props[0]
    others = props[1:]
    label_clause = f":{label}" if label else ""
    # strategy handling simple:
    if strategy == "overwrite":
        # if main is empty, take first non-empty of others
        cypher = f"""
        MATCH (n{label_clause})
        WITH n, coalesce(n.{main}, '') as mainval
        WITH n, mainval, [{', '.join([f"coalesce(n.{p}, '')" for p in others])}] as others
        SET n.{main} = CASE WHEN mainval = '' THEN apoc.coll.filter(others, x->x <> '')[0] ELSE mainval END
        REMOVE {', '.join([f"n.{p}" for p in others])}
        RETURN count(n) AS updated
        """
    elif strategy == "array":
        # combine all into array, unique
        cypher = f"""
        MATCH (n{label_clause})
        WITH n, [v IN [{', '.join([f"n.{p}" for p in [main]+others])}] WHERE v IS NOT NULL AND v <> ''] as vals
        SET n.{main} = apoc.coll.toSet(vals)
        REMOVE {', '.join([f"n.{p}" for p in others])}
        RETURN count(n) AS updated
        """
    elif strategy.startswith("prefix:") or strategy.startswith("suffix:"):
        if strategy.startswith("prefix:"):
            text = strategy.split("prefix:",1)[1]
            cypher = f"""
            MATCH (n{label_clause})
            WITH n
            SET n.{main} = CASE WHEN coalesce(n.{main}, '') = '' THEN coalesce(n.{main}, '') ELSE '{text}' + coalesce(n.{main}, '') END
            RETURN count(n) as updated
            """
        else:
            text = strategy.split("suffix:",1)[1]
            cypher = f"""
            MATCH (n{label_clause})
            WITH n
            SET n.{main} = CASE WHEN coalesce(n.{main}, '') = '' THEN coalesce(n.{main}, '') ELSE coalesce(n.{main}, '') + '{text}' END
            RETURN count(n) as updated
            """
    elif strategy == "sum":
        cypher = f"""
        MATCH (n{label_clause})
        WITH n, toInteger(coalesce(n.{main}, '0')) as mainv, [{', '.join([f"toInteger(coalesce(n.{p}, '0'))" for p in others])}] as others
        SET n.{main} = mainv + reduce(s=0, x IN others | s + x)
        REMOVE {', '.join([f"n.{p}" for p in others])}
        RETURN count(n) as updated
        """
    else:
        return {"error": "unknown strategy"}, 400

    try:
        res = neo.run_cypher(cypher)
    except Exception as e:
        return {"error": str(e)}, 500

    return {"result": res}
