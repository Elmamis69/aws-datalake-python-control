"""
S3 I/O - Operaciones de Lectura y Escritura en S3

Este módulo proporciona funciones utilitarias para:
1. Leer archivos desde buckets S3
2. Convertir archivos JSONL a DataFrames de pandas
3. Escribir DataFrames como archivos Parquet en S3

Es fundamental para el pipeline de transformación de datos:
- Lee archivos RAW (JSONL) desde S3
- Los convierte a formato estructurado (DataFrame)
- Los guarda como Parquet optimizado para consultas

Formatos soportados:
- JSONL (JSON Lines) para datos RAW
- Parquet para datos procesados (más eficiente para análisis)

Uso típico:
    # Leer archivo JSONL
    df = read_jsonl_from_s3('mi-bucket', 'datos/archivo.jsonl')
    
    # Procesar datos...
    
    # Guardar como Parquet
    write_parquet_to_s3(df, 'mi-bucket', 'procesados/archivo.parquet')
"""
import boto3
from typing import Optional
import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)

def read_s3_object(bucket: str, key: str, session: Optional[boto3.Session] = None) -> bytes:
    """
    Descarga un objeto completo de S3 y lo retorna como bytes
    
    Esta es la función base para leer cualquier tipo de archivo de S3.
    Otras funciones especializadas (como read_jsonl_from_s3) la usan internamente.
    
    Args:
        bucket (str): Nombre del bucket S3
        key (str): Ruta/nombre del archivo en S3
        session (boto3.Session, optional): Sesión AWS personalizada
    
    Returns:
        bytes: Contenido completo del archivo
        
    Raises:
        ClientError: Si el archivo no existe o no hay permisos
    """
    s3 = (session or boto3).client('s3')
    logger.info(f"📥 Descargando s3://{bucket}/{key}")
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        logger.info(f"✅ Descargado {len(content)} bytes")
        return content
    except Exception as e:
        logger.error(f"❌ Error descargando archivo: {e}")
        raise

def read_jsonl_from_s3(bucket: str, key: str, session: Optional[boto3.Session] = None) -> pd.DataFrame:
    """
    Lee un archivo JSONL (JSON Lines) de S3 y lo convierte a DataFrame de pandas
    
    JSONL es un formato donde cada línea es un objeto JSON válido.
    Es ideal para datos de eventos o logs porque:
    - Permite streaming (procesar línea por línea)
    - Es resistente a errores (una línea corrupta no afecta las demás)
    - Fácil de generar desde aplicaciones
    
    Ejemplo de archivo JSONL:
        {"timestamp": "2024-01-01", "user_id": 1, "action": "login"}
        {"timestamp": "2024-01-01", "user_id": 2, "action": "logout"}
    
    Args:
        bucket (str): Nombre del bucket S3
        key (str): Ruta del archivo JSONL en S3
        session (boto3.Session, optional): Sesión AWS personalizada
    
    Returns:
        pd.DataFrame: Datos estructurados listos para análisis
        
    Raises:
        ValueError: Si el archivo no es JSONL válido
    """
    logger.info(f"📄 Leyendo archivo JSONL: s3://{bucket}/{key}")
    
    try:
        content = read_s3_object(bucket, key, session)
        # pd.read_json con lines=True maneja formato JSONL
        df = pd.read_json(io.BytesIO(content), lines=True)
        logger.info(f"✅ JSONL leído: {len(df)} filas, {len(df.columns)} columnas")
        return df
    except Exception as e:
        logger.error(f"❌ Error leyendo JSONL: {e}")
        raise

def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str, session: Optional[boto3.Session] = None):
    """
    Escribe un DataFrame como archivo Parquet en S3
    
    Parquet es el formato preferido para datos procesados porque:
    - Compresión eficiente (archivos más pequeños)
    - Consultas rápidas (columnar storage)
    - Compatible con Athena, Spark, y otras herramientas de Big Data
    - Preserva tipos de datos y metadatos
    
    El archivo se guarda sin índice para optimizar el tamaño y compatibilidad.
    
    Args:
        df (pd.DataFrame): DataFrame a guardar
        bucket (str): Nombre del bucket S3 destino
        key (str): Ruta donde guardar el archivo (debe terminar en .parquet)
        session (boto3.Session, optional): Sesión AWS personalizada
    
    Raises:
        ValueError: Si el DataFrame está vacío
        ClientError: Si no hay permisos de escritura en S3
    """
    if df.empty:
        logger.warning("⚠️  DataFrame vacío, no se guardará archivo")
        return
        
    logger.info(f"📤 Guardando DataFrame como Parquet: s3://{bucket}/{key}")
    logger.info(f"   Dimensiones: {len(df)} filas x {len(df.columns)} columnas")
    
    try:
        # Convertir DataFrame a Parquet en memoria
        out_buffer = io.BytesIO()
        df.to_parquet(out_buffer, index=False)  # Sin índice para optimizar
        
        # Subir a S3
        s3 = (session or boto3).client('s3')
        s3.put_object(
            Bucket=bucket, 
            Key=key, 
            Body=out_buffer.getvalue(),
            ContentType='application/octet-stream'  # Tipo MIME para Parquet
        )
        
        # Calcular tamaño del archivo
        file_size = len(out_buffer.getvalue())
        logger.info(f"✅ Parquet guardado exitosamente ({file_size:,} bytes)")
        
    except Exception as e:
        logger.error(f"❌ Error guardando Parquet: {e}")
        raise
