# customer_analysis.py
import duckdb
import pandas as pd
import json
from pathlib import Path
from schema_utils import read_with_schema
import sys
import traceback


# === パス設定 ===
BASE_DIR = Path(__file__).resolve().parent.parent  # 1階層上（20251019_test）
DATA_DIR = BASE_DIR / "data"
SCHEMA_DIR = BASE_DIR / "schemas"
CUSTOMERS_CSV = DATA_DIR / "契約単位_試験用入力データ_10カラム.csv"
SCHEMA_JSON = SCHEMA_DIR / "schema.json"

# === 入力情報読み込み ===
print(f"📂 読み込み中(CSV): {CUSTOMERS_CSV}")
print(f"📂 読み込み中(Schema): {SCHEMA_JSON}")

# === スキーマに基づきCSV読み込み＆バリデーション ===

try:
    df = read_with_schema(CUSTOMERS_CSV, SCHEMA_JSON)
    if not isinstance(df, pd.DataFrame):
        raise TypeError("read_with_schema が pandas.DataFrame を返しませんでした")
except FileNotFoundError as e:
    print(f"❌ ファイルが見つかりません: {e}")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"❌ スキーマ(JSON)の読み込みに失敗しました: {e}")
    sys.exit(1)
except (ValueError, TypeError) as e:
    print(f"❌ 入力データの検証に失敗しました: {e}")
    sys.exit(1)
except Exception as e:
    print("❌ 不明なエラーが発生しました:")
    traceback.print_exc()
    sys.exit(1)

print(f"✅ データ件数: {len(df):,}件\n")

# === DuckDB接続 ===
con = duckdb.connect()
con.register("契約ファイル", df)

print("=== セグメント別顧客数 ===")
seg_result = con.sql("""
                     SELECT 
                     氏名コード, 
                     成績年月, 
                     SUM(修Ｓ) AS 総修Ｓ, 
                     SUM(収入Ｐ) AS 総収入Ｐ
                     FROM 契約ファイル
                     GROUP BY 氏名コード, 成績年月
                     ORDER BY 氏名コード, 成績年月
                     """).df()
print(seg_result, "\n")

# === 終了 ===
print("🎉 分析完了！")