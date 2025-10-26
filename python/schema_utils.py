import json
import re
import pandas as pd


def load_schema(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _build_read_csv_kwargs(schema: dict):
    # まず全列は文字列で読み込んでから整形するのが安全（型崩れ防止）
    usecols = [f["name"] for f in schema["fields"]]
    return {
        "dtype": {col: "string" for col in usecols},
        "usecols": usecols,
        "encoding": schema.get("format", {}).get("encoding", "utf-8"),
    }

def _coerce_types(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    for field in schema["fields"]:
        name = field["name"]
        ftype = field["type"]
        ptype = field.get("pandasType")
        logical = field.get("logicalType")

        # 前処理（トリム）
        if name in df.columns and pd.api.types.is_string_dtype(df[name]):
            df[name] = df[name].str.strip()

        # logicalType の処理
        if logical == "zeroPad7":
            df[name] = df[name].str.zfill(7)
        if logical == "yearMonth":
            # "YYYYMM" を Period や datetime-like にするならここで
            # df[name+"_period"] = pd.PeriodIndex(df[name], freq="M")  # 必要なら追加列化
            pass

        # 型変換
        if ftype == "integer":
            target = ptype or "Int64"
            df[name] = pd.to_numeric(df[name], errors="coerce").astype(target)
        elif ftype == "number":
            df[name] = pd.to_numeric(df[name], errors="coerce")
        elif ftype == "string":
            df[name] = df[name].astype("string")

        # enum を category にしておくと便利
        if "enum" in field and ftype == "string":
            df[name] = pd.Categorical(df[name], categories=field["enum"])

    return df

def _validate(df: pd.DataFrame, schema: dict):
    errors = []

    # 必須・nullチェック・パターン・enum・範囲
    for field in schema["fields"]:
        name = field["name"]
        required = field.get("required", False)
        nullable = field.get("nullable", True)
        pattern = field.get("pattern")
        enum = field.get("enum")
        minv = field.get("min")
        maxv = field.get("max")

        if required and name not in df.columns:
            errors.append(f"[schema] required column missing: {name}")
            continue

        if name not in df.columns:
            continue

        s = df[name]

        if not nullable and s.isna().any():
            ix = s[s.isna()].index.tolist()[:5]
            errors.append(f"[null] {name} has nulls at rows {ix} (showing up to 5)")

        if pattern and pd.api.types.is_string_dtype(s):
            mism = ~s.fillna("").str.match(re.compile(pattern))
            if mism.any():
                ix = s[mism].index.tolist()[:5]
                bad = s[mism].head(5).tolist()
                errors.append(f"[pattern] {name} failed pattern {pattern}: rows {ix}, values {bad}")

        if enum is not None:
            mism = ~s.isin(enum)
            # 欠損は別途扱う
            mism = mism & s.notna()
            if mism.any():
                ix = s[mism].index.tolist()[:5]
                bad = s[mism].head(5).tolist()
                errors.append(f"[enum] {name} not in {enum}: rows {ix}, values {bad}")

        if pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s):
            if minv is not None:
                mism = s.notna() & (s < minv)
                if mism.any():
                    ix = s[mism].index.tolist()[:5]
                    bad = s[mism].head(5).tolist()
                    errors.append(f"[range] {name} < {minv}: rows {ix}, values {bad}")
            if maxv is not None:
                mism = s.notna() & (s > maxv)
                if mism.any():
                    ix = s[mism].index.tolist()[:5]
                    bad = s[mism].head(5).tolist()
                    errors.append(f"[range] {name} > {maxv}: rows {ix}, values {bad}")

    # 主キー重複チェック
    pk = schema.get("primaryKey", [])
    if pk:
        dup = df.duplicated(subset=pk, keep=False)
        if dup.any():
            ix = df[dup].index.tolist()[:10]
            keys_preview = df.loc[ix, pk].to_dict(orient="records")
            errors.append(f"[primaryKey] duplicates on {pk}: rows {ix}, keys {keys_preview}")

    if errors:
        raise ValueError("Schema validation failed:\n- " + "\n- ".join(errors))

def read_with_schema(csv_path: str, schema_path: str) -> pd.DataFrame:
    schema = load_schema(schema_path)
    read_kwargs = _build_read_csv_kwargs(schema)
    try:
        df = pd.read_csv(csv_path, **read_kwargs)
    except ValueError as e:
        # pandas の usecols 等による読み込み時エラーを検知して
        # スキーマ検証エラーとして統一的なメッセージで再送出する
        raise ValueError("required column missing or CSV reading error: " + str(e))
    df = _coerce_types(df, schema)
    _validate(df, schema)
    return df
