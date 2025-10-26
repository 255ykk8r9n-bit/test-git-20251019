# customer_analysis.py
import duckdb
import pandas as pd
import json
from pathlib import Path
from schema_utils import read_with_schema, apply_schema_from_path
from tables_config_utils import register_tables_from_config
from sql_generate_utils import build_duckdb_sql_from_process
import sys
import traceback
import time


def main():
    # === パス設定 ===
    BASE_DIR = Path(__file__).resolve().parent.parent  # 1階層上（20251019_test）
    DATA_DIR = BASE_DIR / "data"
    SCHEMA_DIR = BASE_DIR / "json" / "schemas"
    PROCESS_PRM_DIR = BASE_DIR / "json" / "process_prm"
    TABLES_PRM_DIR = BASE_DIR / "json" / "tables_prm"

    INPUT_CSV = DATA_DIR / "contract_data_test_10rec_noheader.csv"
    SCHEMA_JSON_INP = SCHEMA_DIR / "schema_contract_data_test_noheader.json"

    OUTPUT_CSV = DATA_DIR / "agent_data_test.csv"
    SCHEMA_JSON_OUT = SCHEMA_DIR / "schema_agent_data_test.json"

    PROCESS_PRM = PROCESS_PRM_DIR / "process_contract_to_agent.json"
    TABLES_PRM = TABLES_PRM_DIR / "tables.json"


    start_all = time.perf_counter()  # 総処理開始タイムスタンプ

    print(f"📂 読込中(CSV): {INPUT_CSV}")
    print(f"📂 読込中(Schema): {SCHEMA_JSON_INP}")

    t0 = time.perf_counter()
    try:
        # df = read_with_schema(INPUT_CSV, SCHEMA_JSON_INP)
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
    print(f"✅ 読込完了: {len(df):,}件")
    print(f"\n")

    # DuckDB
    t_sql = t_schema = 0.0
    con = None
    try:
        con = duckdb.connect()
        register_tables_from_config(con, TABLES_PRM, df)

        t1 = time.perf_counter()
        sql = build_duckdb_sql_from_process(str(PROCESS_PRM))
        print(f"📋 実行SQL:\n{sql}\n")

        sql_result_tmp = con.sql(sql).df()
        t_sql = time.perf_counter() - t1

        t1 = time.perf_counter()
        sql_result = apply_schema_from_path(sql_result_tmp, SCHEMA_JSON_OUT)
        t_schema = time.perf_counter() - t1

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
    print(f"📂 書出中(CSV): {OUTPUT_CSV}")
    print(f"📂 書出中(Schema): {SCHEMA_JSON_OUT}")
    t1 = time.perf_counter()
    sql_result.to_csv(OUTPUT_CSV, index=False)
    t_output = time.perf_counter() - t1
    print(f"✅ 書出完了: {len(sql_result):,}件")
    print(f"\n")

    # タイムログ
    print(f"⏱ 入力読み込み＋検証: {read_time:.3f}s")
    print(f"⏱ クエリ実行時間: {t_sql:.3f}s")
    print(f"⏱ 出力スキーマ適用: {t_schema:.3f}s")
    print(f"⏱ 出力書き込み: {t_output:.3f}s")
    print(f"⏱ 総処理時間: {total_time:.3f}s")
    print(f"\n")

    #終了
    print("🎉 分析完了！")

if __name__ == "__main__":
    main()