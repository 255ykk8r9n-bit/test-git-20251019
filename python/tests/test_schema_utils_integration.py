import json
from pathlib import Path
import pandas as pd
import pytest
from schema_utils import read_with_schema

def _write_file(p: Path, text: str):
    p.write_text(text, encoding="utf-8")
    return str(p)

def _default_schema():
    return {
        "name": "契約集計入力",
        "version": "1.0.0",
        "format": {"filetype": "csv", "encoding": "utf-8", "delimiter": ",", "header": True},
        "primaryKey": ["証券記号番号"],
        "fields": [
            {"name": "氏名コード", "type": "string", "required": True, "logicalType": "zeroPad7"},
            {"name": "成績年月", "type": "string", "required": True, "logicalType": "yearMonth"},
            {"name": "証券記号番号", "type": "string", "required": True},
            {"name": "保険金", "type": "integer", "required": True, "nullable": False, "pandasType": "Int64"},
            {"name": "保険料", "type": "integer", "required": True, "nullable": False, "pandasType": "Int64"},
            {"name": "件数", "type": "integer", "required": True, "nullable": False, "pandasType": "Int16"},
            {"name": "新規契約フラグ", "type": "integer", "required": True, "pandasType": "Int8", "logicalType": "booleanInt"},
            {"name": "更改フラグ", "type": "integer", "required": True, "pandasType": "Int8", "logicalType": "booleanInt"},
            {"name": "解約フラグ", "type": "integer", "required": True, "pandasType": "Int8", "logicalType": "booleanInt"},
            {"name": "カク職表示", "type": "integer", "required": True, "pandasType": "Int8", "logicalType": "booleanInt"},
            {"name": "チャネル区分", "type": "string", "required": True}
        ]
    }

def test_read_with_schema_success(tmp_path):
    schema = _default_schema()
    schema_path = tmp_path / "schema.json"
    _write_file(schema_path, json.dumps(schema, ensure_ascii=False))

    # CSV ヘッダは schema の fields 順に合わせる
    header = ",".join([f["name"] for f in schema["fields"]])
    # 氏名コード は先頭 0 を持たない値で書き、zeroPad7 の効果を確認
    rows = [
        "123,202509,10000001,1000000,12000,1,1,0,0,0,2",
        "45,202510,10000002,2000000,8000,1,0,1,0,0,1"
    ]
    csv_path = tmp_path / "data.csv"
    _write_file(csv_path, header + "\n" + "\n".join(rows) + "\n")

    df = read_with_schema(str(csv_path), str(schema_path))

    # zeroPad7 が効いていること
    assert df.loc[0, "氏名コード"] == "0000123"
    assert df.loc[1, "氏名コード"] == "0000045"

    # 整数列が pandas の nullable integer に変換されている（dtype 名に Int が含まれる）
    assert "Int64" in str(df["保険金"].dtype) or pd.api.types.is_integer_dtype(df["保険金"])
    assert "Int16" in str(df["件数"].dtype) or pd.api.types.is_integer_dtype(df["件数"])
    # 欠損がないこと（nullable False 指定の列）
    assert df["保険金"].isna().sum() == 0

def test_missing_required_raises(tmp_path):
    schema = _default_schema()
    # 必須列を残さない CSV を作る（氏名コードを落とす）
    schema_path = tmp_path / "schema.json"
    _write_file(schema_path, json.dumps(schema, ensure_ascii=False))

    header_cols = [f["name"] for f in schema["fields"] if f["name"] != "氏名コード"]
    csv_path = tmp_path / "data_missing.csv"
    _write_file(csv_path, ",".join(header_cols) + "\n" + "202509,10000001,1000000,12000,1,1,0,0,2\n")

    with pytest.raises(ValueError) as exc:
        read_with_schema(str(csv_path), str(schema_path))
    assert "required column missing" in str(exc.value)

def test_primary_key_duplicate_raises(tmp_path):
    schema = _default_schema()
    schema_path = tmp_path / "schema.json"
    _write_file(schema_path, json.dumps(schema, ensure_ascii=False))

    header = ",".join([f["name"] for f in schema["fields"]])
    # 証券記号番号 を重複させる
    rows = [
        "123,202509,10000001,1000000,12000,1,1,0,0,0,2",
        "123,202510,10000001,2000000,8000,1,0,1,0,0,1"
    ]
    csv_path = tmp_path / "data_dup.csv"
    _write_file(csv_path, header + "\n" + "\n".join(rows) + "\n")

    with pytest.raises(ValueError) as exc:
        read_with_schema(str(csv_path), str(schema_path))
    assert "primaryKey" in str(exc.value)