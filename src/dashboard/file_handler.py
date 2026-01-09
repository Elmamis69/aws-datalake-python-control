"""
Módulo para manejo de archivos S3
"""

import boto3
import pandas as pd
import io
from datetime import datetime

def get_files_advanced_filter(aws_conf, selected_date=None, file_type=None, source='processed'):
    """Obtener archivos con filtros avanzados"""
    try:
        s3 = boto3.client('s3')
        
        # Seleccionar bucket según el origen
        if source == 'processed':
            bucket = aws_conf['s3_processed_bucket']
            prefix = aws_conf['s3_processed_prefix']
        elif source == 'raw':
            bucket = aws_conf['s3_raw_bucket']
            prefix = aws_conf['s3_raw_prefix']
        else:  # 'all'
            bucket = aws_conf['s3_raw_bucket']
            prefix = ''
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        filtered_files = []
        
        for obj in response.get('Contents', []):
            # Filtrar por fecha si se especifica
            if selected_date and obj['LastModified'].date() != selected_date:
                continue
            
            # Filtrar por tipo de archivo si se especifica
            if file_type and file_type != 'todos':
                ext = obj['Key'].split('.')[-1].lower() if '.' in obj['Key'] else 'sin_ext'
                if ext != file_type:
                    continue
            
            filtered_files.append({
                'nombre': obj['Key'].split('/')[-1],
                'ruta': obj['Key'],
                'bucket': bucket,
                'tamaño': format_size(obj['Size']),
                'fecha': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                'tipo': obj['Key'].split('.')[-1].lower() if '.' in obj['Key'] else 'sin_ext'
            })
        
        # Ordenar por fecha (más recientes primero)
        filtered_files.sort(key=lambda x: x['fecha'], reverse=True)
        
        return filtered_files
    except Exception:
        return []

def read_file_from_s3(bucket, key, file_type):
    """Leer archivo desde S3 y retornar DataFrame"""
    try:
        s3_path = f"s3://{bucket}/{key}"
        
        if file_type == 'parquet':
            df = pd.read_parquet(s3_path)
        elif file_type in ['json', 'jsonl']:
            df = pd.read_json(s3_path, lines=True if file_type == 'jsonl' else False)
        elif file_type == 'csv':
            df = pd.read_csv(s3_path)
        else:
            return None, f"Tipo de archivo no soportado: {file_type}"
        
        return df, None
    except Exception as e:
        return None, f"Error leyendo archivo: {str(e)}"

def download_file_from_s3(bucket, key):
    """Descargar archivo desde S3 y retornar contenido"""
    try:
        s3 = boto3.client('s3')
        
        # Crear buffer en memoria
        buffer = io.BytesIO()
        s3.download_fileobj(bucket, key, buffer)
        buffer.seek(0)
        
        return buffer.getvalue(), None
    except Exception as e:
        return None, f"Error descargando archivo: {str(e)}"

def format_size(size_bytes):
    """Formatear bytes a formato legible"""
    if size_bytes == 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"