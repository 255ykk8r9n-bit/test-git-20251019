import json
from pathlib import Path

# ① 許可リスト（ホワイトリスト）
_ALLOWED_AGG_FUNCS    = {"SUM","COUNT","AVG","MIN","MAX"}
_ALLOWED_SCALAR_FUNCS = {"COALESCE","ABS","ROUND","FLOOR","CEIL","NULLIF"}  # 必要に応じて追加
_ALLOWED_FUNCS        = _ALLOWED_AGG_FUNCS | _ALLOWED_SCALAR_FUNCS

# BETWEEN / NULL 系を where 用にも許容（任意）
_ALLOWED_OPS = {"=","!=","<",">","<=",">=","IN","NOT IN","LIKE","BETWEEN","IS NULL","IS NOT NULL"}


def _q_ident(name: str) -> str:
    # 識別子を "二重引用符" で適切にクオート（内部の " は "" にエスケープ）
    return '"' + str(name).replace('"','""') + '"'

def _lit(val):
    # リテラル（数値はそのまま、文字列は '単引用符' でクオート）
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    # 文字列
    s = str(val).replace("'", "''")
    return f"'{s}'"


def load_process(path: str | Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"process_prm file not found: {path}") from e
    except PermissionError as e:
        raise PermissionError(f"Permission denied reading process_prm file: {path}") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in process_prm file {path}: {e.msg} (line {e.lineno} col {e.colno})") from e
    except OSError as e:
        raise OSError(f"Failed to read process_prm file {path}: {e}") from e
    if not isinstance(data, dict):
        raise TypeError(f"process_prm root must be a JSON object (dict), got {type(data).__name__}")
    return data

def _build_where(where_specs: list[dict]) -> str:
    parts = []
    for w in where_specs or []:
        col = _q_ident(w["column"])
        op  = w["op"].upper().strip()
        if op not in _ALLOWED_OPS:
            raise ValueError(f"Unsupported operator: {op}")
        if op in {"IN","NOT IN"}:
            vals = w.get("values") or w.get("value")
            if not isinstance(vals, (list, tuple)):
                raise ValueError(f"{op} requires list of values")
            vals_sql = ", ".join(_lit(v) for v in vals)
            parts.append(f"{col} {op} ({vals_sql})")
        else:
            parts.append(f"{col} {op} {_lit(w['value'])}")
    return "" if not parts else "WHERE " + " AND ".join(parts)

def _build_expr(expr, *, in_scope=frozenset()) -> str:
    """JSONで表現された式(expr)をSQL文字列に展開する"""
    if isinstance(expr, str):
        return _q_ident(expr)

    if "col" in expr:
        return _q_ident(expr["col"])

    if "val" in expr:
        return _lit(expr["val"])

    if "ref" in expr:
        name = expr["ref"]
        if name in in_scope:
            return _q_ident(name)
        raise ValueError(f"Undefined ref: {name}")

    if "fn" in expr:
        fn = expr["fn"].upper()
        if fn not in _ALLOWED_FUNCS:
            raise ValueError(f"Unsupported function: {fn}")
        args = ", ".join(_build_expr(a, in_scope=in_scope) for a in expr.get("args", []))
        return f"{fn}({args})"

    if "op" in expr:
        left, right = expr.get("args", [None, None])
        return f"({_build_expr(left, in_scope=in_scope)} {expr['op']} {_build_expr(right, in_scope=in_scope)})"

    if "case" in expr:
        whens = " ".join(
            f"WHEN {_build_condition(w['when'])} THEN {_build_expr(w['then'], in_scope=in_scope)}"
            for w in expr["case"]["whens"]
        )
        else_part = ""
        if "else" in expr["case"]:
            else_part = f" ELSE {_build_expr(expr['case']['else'], in_scope=in_scope)}"
        return f"(CASE {whens}{else_part} END)"

    raise ValueError(f"Unsupported expression format: {expr}")

def _build_condition(cond):
    """条件式(cond)をSQL文字列に変換する"""
    if "all" in cond:  # AND結合
        return "(" + " AND ".join(_build_condition(c) for c in cond["all"]) + ")"
    if "any" in cond:  # OR結合
        return "(" + " OR ".join(_build_condition(c) for c in cond["any"]) + ")"
    if "not" in cond:  # 否定
        return "(NOT " + _build_condition(cond["not"]) + ")"

    # 単純比較式
    col = _q_ident(cond["column"])
    op  = cond["op"].upper()
    val = cond.get("value")

    # NULL系だけは特別扱い
    if op in ("IS NULL", "IS NOT NULL"):
        return f"{col} {op}"

    # IN/NOT IN
    if op in ("IN", "NOT IN"):
        if not isinstance(val, (list, tuple)):
            raise ValueError(f"{op} requires list value: {val}")
        vals = ", ".join(_lit(v) for v in val)
        return f"{col} {op} ({vals})"

    # BETWEEN
    if op == "BETWEEN":
        low, high = val
        return f"{col} BETWEEN {_lit(low)} AND {_lit(high)}"

    # 通常の二項比較 (=, !=, >, >=, <, <=, LIKE)
    return f"{col} {op} {_lit(val)}"


def build_duckdb_sql_from_process(path: str) -> str:
    proc = load_process(path)
    src = _q_ident(proc["source"])
    where_sql_inner = _build_where(proc.get("where"))

    # CTE句（必要なら）
    cte_cols, in_scope = [], set()
    for ne in proc.get("named_expressions", []):
        expr_sql = _build_expr(ne["expr"], in_scope=frozenset(in_scope))
        cte_cols.append(f"{expr_sql} AS {_q_ident(ne['name'])}")
        in_scope.add(ne["name"])  # 以降は ref で参照可能

    if cte_cols:
        inner_select = f"SELECT *, {', '.join(cte_cols)} FROM {src}{(' ' + where_sql_inner) if where_sql_inner else ''}"
        from_sql = "base"
        with_sql = f"WITH base AS ({inner_select})\n"
        where_sql_outer = ""  # ✅ CTEを使う場合、外側ではWHEREをもう一度付けない
    else:
        from_sql = str(src)
        with_sql = ""
        where_sql_outer = where_sql_inner

    # SELECT句
    selects = []
    for g in proc["group_by"]:
        selects.append(_q_ident(g))

    # 外側SELECTで参照可能な名前（named + group_by）
    in_scope_outer = frozenset(in_scope | set(proc["group_by"]))

    for a in proc.get("aggregations", []):
        fn  = a["fn"].upper()
        if fn not in _ALLOWED_AGG_FUNCS:
            raise ValueError(f"Unsupported aggregation: {fn}")
        expr_sql = _build_expr(a["expr"], in_scope=in_scope_outer)  # ✅ scope 伝播
        alias = _q_ident(a["alias"])
        selects.append(f"{fn}({expr_sql}) AS {alias}")

    select_sql = ",\n    ".join(selects)

    # GROUP BY
    gb_cols = ", ".join(_q_ident(c) for c in proc["group_by"])
    group_sql = f"GROUP BY {gb_cols}" if gb_cols else ""

    # ORDER BY（expr も許容：文字列→識別子、dict→_build_expr、数字→位置指定）
    order_specs = proc.get("order_by") or []
    if order_specs:
        ob_parts = []
        for ob in order_specs:
            ob_expr = ob["expr"]
            if isinstance(ob_expr, int) or (isinstance(ob_expr, str) and ob_expr.isdigit()):
                expr_sql = str(ob_expr)                     # 位置指定
            elif isinstance(ob_expr, dict):
                expr_sql = _build_expr(ob_expr, in_scope=in_scope_outer)
            else:
                expr_sql = _q_ident(ob_expr)               # 文字列は列名として
            asc = ob.get("asc", True)
            ob_parts.append(f"{expr_sql} {'ASC' if asc else 'DESC'}")
        order_sql = "ORDER BY " + ", ".join(ob_parts)
    else:
        order_sql = ""

    limit = proc.get("limit")
    limit_sql = f"LIMIT {int(limit)}" if isinstance(limit, int) else ""

    outer_sql = f"""SELECT
        {select_sql}
        FROM {from_sql}
        {where_sql_outer}
        {group_sql}
        {order_sql}
        {limit_sql}
    """.strip()

    return with_sql + outer_sql

