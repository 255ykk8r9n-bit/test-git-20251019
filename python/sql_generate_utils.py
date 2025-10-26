import json
from pathlib import Path

# ① 許可リスト（ホワイトリスト）
_ALLOWED_FUNCS = {"SUM","COUNT","AVG","MIN","MAX"}
_ALLOWED_OPS   = {"=","!=","<",">","<=",">=","IN","NOT IN","LIKE"}

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

def build_duckdb_sql_from_process(path: str) -> str:
    proc = load_process(path)
    src = _q_ident(proc["source"])
    # SELECT句
    selects = []
    # group by 列
    for g in proc["group_by"]:
        selects.append(_q_ident(g))
    # 集計列
    for a in proc.get("aggregations", []):
        fn  = a["fn"].upper()
        if fn not in _ALLOWED_FUNCS:
            raise ValueError(f"Unsupported aggregation: {fn}")
        expr = _q_ident(a["expr"])
        alias = _q_ident(a["alias"])
        selects.append(f"{fn}({expr}) AS {alias}")

    select_sql = ",\n    ".join(selects)
    where_sql  = _build_where(proc.get("where"))
    # GROUP BY
    gb_cols = ", ".join(_q_ident(c) for c in proc["group_by"])
    group_sql = f"GROUP BY {gb_cols}" if gb_cols else ""
    # ORDER BY
    order_specs = proc.get("order_by") or []
    if order_specs:
        ob_parts = []
        for ob in order_specs:
            expr = ob["expr"] 
            if str(expr).isdigit():
                expr_sql = expr
            else:
                expr_sql = _q_ident(expr)
            asc = ob.get("asc", True)
            ob_parts.append(f"{expr_sql} {'ASC' if asc else 'DESC'}")
        order_sql = "ORDER BY " + ", ".join(ob_parts)
    else:
        order_sql = ""

    limit = proc.get("limit")
    limit_sql = f"LIMIT {int(limit)}" if isinstance(limit, int) else ""

    sql = f"""SELECT
    {select_sql}
FROM {src}
{where_sql}
{group_sql}
{order_sql}
{limit_sql}
""".strip()  # 余計な空行はDuckDBは気にしませんが整形
    return sql
