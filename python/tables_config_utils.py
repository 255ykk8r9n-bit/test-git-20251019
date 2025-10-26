from pathlib import Path
import json
import duckdb
import pandas as pd

def register_tables_from_config(con: duckdb.DuckDBPyConnection, config_path: Path, df: pd.DataFrame):
    """
    tables.json からテーブル定義を読み込み、DuckDBに register() 登録する。
    """
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    for t in cfg.get("tables", []):
        name = t["name"]
        src_type = t.get("source_type", "csv")
        path = Path(t["path"])
        schema_path = Path(t["schema"])
        if src_type.lower() != "csv":
            raise ValueError(f"Unsupported source_type: {src_type} for table {name}")
        con.register(name, df)
        print(f"✅ Registered table '{name}' ({len(df):,} rows)")
