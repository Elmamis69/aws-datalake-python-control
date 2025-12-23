"""
transform.py
Lógica de transformación: JSONL → DataFrame → particionado → Parquet
"""
import pandas as pd
from typing import Optional

def transform_jsonl_to_parquet(
    df: pd.DataFrame,
    date_field: str = 'event_time',
    partition_by_date: bool = True
) -> dict:
    """
    Transforma un DataFrame: agrega columnas de partición por fecha si se requiere.
    Retorna un dict con claves: 'df' (DataFrame transformado), 'partition_path' (str)
    """
    if partition_by_date and date_field in df.columns:
        df[date_field] = pd.to_datetime(df[date_field])
        year = df[date_field].dt.year.iloc[0]
        month = df[date_field].dt.month.iloc[0]
        day = df[date_field].dt.day.iloc[0]
        partition_path = f"year={year}/month={month:02d}/day={day:02d}/"
    else:
        partition_path = ""
    return {'df': df, 'partition_path': partition_path}
