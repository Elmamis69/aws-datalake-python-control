"""
s3_io.py
Helpers para leer y escribir archivos en S3 usando boto3.
"""
import boto3
from typing import Optional
import pandas as pd
import io

def read_s3_object(bucket: str, key: str, session: Optional[boto3.Session] = None) -> bytes:
    """
    Descarga un objeto de S3 y lo retorna como bytes.
    """
    s3 = (session or boto3).client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()

def read_jsonl_from_s3(bucket: str, key: str, session: Optional[boto3.Session] = None) -> pd.DataFrame:
    """
    Lee un archivo JSONL de S3 y lo convierte a DataFrame.
    """
    content = read_s3_object(bucket, key, session)
    return pd.read_json(io.BytesIO(content), lines=True)

def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str, session: Optional[boto3.Session] = None):
    """
    Escribe un DataFrame como Parquet en S3.
    """
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)
    s3 = (session or boto3).client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=out_buffer.getvalue())
