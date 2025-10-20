# customer_analysis.py
import duckdb
import pandas as pd
from pathlib import Path

# === パス設定 ===
BASE_DIR = Path(__file__).resolve().parent.parent  # 1階層上（20251019_test）
DATA_DIR = BASE_DIR / "data"
CUSTOMERS_CSV = DATA_DIR / "customers__preview.csv"

# === CSV読み込み ===
print(f"📂 読み込み中: {CUSTOMERS_CSV}")
df = pd.read_csv(CUSTOMERS_CSV)
print(f"✅ データ件数: {len(df):,}件\n")

# === DuckDB接続 ===
con = duckdb.connect()
con.register("customers", df)

# === 分析①：セグメント別顧客数 ===
print("=== セグメント別顧客数 ===")
seg_result = con.sql("""
SELECT segment, COUNT(*) AS customer_count
FROM customers
GROUP BY segment
ORDER BY customer_count DESC
""").df()
print(seg_result, "\n")

# === 分析②：都道府県別 顧客数 TOP5 ===
print("=== 都道府県別 顧客数TOP5 ===")
pref_result = con.sql("""
SELECT prefecture, COUNT(*) AS customer_count
FROM customers
GROUP BY prefecture
ORDER BY customer_count DESC
LIMIT 5
""").df()
print(pref_result, "\n")

# === 分析③：プレミアム顧客の割合 ===
print("=== プレミアム顧客の割合 ===")
ratio_result = con.sql("""
SELECT
  100.0 * SUM(CASE WHEN segment = 'プレミアム' THEN 1 ELSE 0 END) / COUNT(*) AS premium_ratio_percent
FROM customers
""").df()
print(ratio_result, "\n")

# === 終了 ===
print("🎉 分析完了！")
