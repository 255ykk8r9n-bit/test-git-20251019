# customer_analysis.py
import duckdb
import pandas as pd
import json
from pathlib import Path
from schema_utils import read_with_schema, apply_schema_from_path
import sys
import traceback
import time


def main():
    # === パス設定 ===
    BASE_DIR = Path(__file__).resolve().parent.parent  # 1階層上（20251019_test）
    DATA_DIR = BASE_DIR / "data"
    SCHEMA_DIR = BASE_DIR / "schemas"
    INPUT_CSV = DATA_DIR / "contract_data_test_10rec.csv"
    SCHEMA_JSON_INP = SCHEMA_DIR / "schema_contract_data_test.json"
    OUTPUT_CSV = DATA_DIR / "agent_data_test.csv"
    SCHEMA_JSON_OUT = SCHEMA_DIR / "schema_agent_data_test.json"

    start_all = time.perf_counter()  # 総処理開始タイムスタンプ

    print(f"📂 読み込み中(CSV): {INPUT_CSV}")
    print(f"📂 読み込み中(Schema): {SCHEMA_JSON_INP}")

    t0 = time.perf_counter()
    try:
        df = read_with_schema(INPUT_CSV, SCHEMA_JSON_INP)
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
    except Exception:
        print("❌ 不明なエラーが発生しました:")
        traceback.print_exc()
        sys.exit(1)
    read_time = time.perf_counter() - t0
    print(f"⏱ 入力読み込み＋検証: {read_time:.3f}s")
    print(f"✅ データ件数: {len(df):,}件\n")

    # DuckDB
    t_sql = t_schema = 0.0
    con = None
    try:
        con = duckdb.connect()
        con.register("契約ファイル", df)

        print("=== 氏名コード×成績年月の集計 ===")
        t1 = time.perf_counter()
        sql_result_tmp = con.sql("""
            SELECT
                "氏名コード",
                "成績年月",
                SUM("修Ｓ")   AS "総修Ｓ",
                SUM("収入Ｐ") AS "総収入Ｐ"
            FROM "契約ファイル"
            GROUP BY 1, 2
            ORDER BY 1, 2
        """).df()
        t_sql = time.perf_counter() - t1

        t2 = time.perf_counter()
        sql_result = apply_schema_from_path(sql_result_tmp, SCHEMA_JSON_OUT)
        t_schema = time.perf_counter() - t2

    except duckdb.Error as e:
        print(f"❌ DuckDB処理に失敗しました: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"❌ スキーマ適用に失敗しました: {e}")
        sys.exit(1)
    except Exception:
        print("❌ 集計処理で不明なエラーが発生しました:")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if con is not None:
            con.close()

    total_time = time.perf_counter() - start_all

    # 出力
    print(sql_result.head())  # 全量は保存に回すなど
    print(f"⏱ クエリ実行時間: {t_sql:.3f}s")
    print(f"⏱ 出力スキーマ適用: {t_schema:.3f}s")
    print("🎉 分析完了！")
    print(f"⏱ 総処理時間: {total_time:.3f}s")

if __name__ == "__main__":
    main()