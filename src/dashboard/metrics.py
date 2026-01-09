"""
Módulo para obtener métricas del data lake
"""

import boto3
import pandas as pd
from datetime import datetime, date, timedelta
import psutil
import os
from pathlib import Path

def get_metrics(config):
    """Obtener métricas del data lake"""
    try:
        from scripts.run_monitor import DataLakeMonitor
        
        monitor = DataLakeMonitor(config)
        aws_conf = config['aws']
        
        # Obtener datos básicos
        raw_count, raw_size = monitor.count_s3_objects(
            aws_conf['s3_raw_bucket'], 
            aws_conf['s3_raw_prefix']
        )
        
        processed_count, processed_size = monitor.count_s3_objects(
            aws_conf['s3_processed_bucket'], 
            aws_conf['s3_processed_prefix']
        )
        
        queue_messages = monitor.get_sqs_message_count(aws_conf['sqs_queue_url'])
        recent_errors = monitor.get_recent_logs(24)
        
        # Métricas adicionales
        recent_files = get_recent_processed_files(aws_conf)
        today_files = count_files_today(aws_conf)
        worker_status = check_worker_status()
        file_stats = get_file_type_stats(aws_conf)
        folder_stats = get_folder_distribution(aws_conf)
        
        return {
            'raw_files': raw_count,
            'raw_size': raw_size,
            'processed_files': processed_count,
            'processed_size': processed_size,
            'total_size': raw_size + processed_size,
            'queue_messages': queue_messages,
            'errors': recent_errors,
            'recent_files': recent_files,
            'today_files': today_files,
            'worker_status': worker_status,
            'file_stats': file_stats,
            'folder_stats': folder_stats,
            'aws_conf': aws_conf,
            'timestamp': datetime.now()
        }
    except Exception as e:
        return None

def get_recent_processed_files(aws_conf):
    """Obtener los últimos 5 archivos procesados"""
    try:
        s3 = boto3.client('s3')
        
        response = s3.list_objects_v2(
            Bucket=aws_conf['s3_processed_bucket'],
            Prefix=aws_conf['s3_processed_prefix'],
            MaxKeys=50
        )
        
        if 'Contents' not in response:
            return []
        
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        
        recent = []
        for file in files[:5]:
            recent.append({
                'nombre': file['Key'].split('/')[-1],
                'ruta': file['Key'],
                'tamaño': format_size(file['Size']),
                'fecha': file['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return recent
    except Exception:
        return []

def count_files_today(aws_conf):
    """Contar archivos procesados hoy"""
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(
            Bucket=aws_conf['s3_processed_bucket'],
            Prefix=aws_conf['s3_processed_prefix']
        )
        
        if 'Contents' not in response:
            return 0
        
        today = date.today()
        count = 0
        
        for obj in response['Contents']:
            if obj['LastModified'].date() == today:
                count += 1
        
        return count
    except Exception:
        return 0

def get_file_type_stats(aws_conf):
    """Obtener estadísticas por tipo de archivo"""
    try:
        s3 = boto3.client('s3')
        
        # RAW bucket
        raw_response = s3.list_objects_v2(Bucket=aws_conf['s3_raw_bucket'])
        
        # Processed bucket
        processed_response = s3.list_objects_v2(
            Bucket=aws_conf['s3_processed_bucket'],
            Prefix=aws_conf['s3_processed_prefix']
        )
        
        file_types = {}
        
        # Contar tipos en RAW
        for obj in raw_response.get('Contents', []):
            ext = obj['Key'].split('.')[-1].lower() if '.' in obj['Key'] else 'sin_ext'
            file_types[ext] = file_types.get(ext, 0) + 1
        
        # Contar tipos en procesados
        for obj in processed_response.get('Contents', []):
            ext = obj['Key'].split('.')[-1].lower() if '.' in obj['Key'] else 'sin_ext'
            if ext == 'parquet':
                file_types['parquet'] = file_types.get('parquet', 0) + 1
        
        return file_types
    except Exception:
        return {'csv': 0, 'json': 0, 'parquet': 0}

def get_folder_distribution(aws_conf):
    """Obtener distribución por carpetas"""
    try:
        s3 = boto3.client('s3')
        
        folders = {}
        
        # RAW bucket
        raw_response = s3.list_objects_v2(Bucket=aws_conf['s3_raw_bucket'])
        
        for obj in raw_response.get('Contents', []):
            parts = obj['Key'].split('/')
            if len(parts) > 1:
                folder = parts[0] + '/'
                folders[folder] = folders.get(folder, 0) + 1
        
        # Processed bucket
        processed_response = s3.list_objects_v2(
            Bucket=aws_conf['s3_processed_bucket'],
            Prefix=aws_conf['s3_processed_prefix']
        )
        
        for obj in processed_response.get('Contents', []):
            parts = obj['Key'].split('/')
            if len(parts) > 1:
                folder = 'processed/'
                folders[folder] = folders.get(folder, 0) + 1
        
        return folders
    except Exception:
        return {'raw/': 0, 'processed/': 0}

def check_worker_status():
    """Verificar si el worker está corriendo"""
    try:
        current_dir = str(Path(__file__).parent.parent.parent)
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
            try:
                if proc.info['name'] and 'python' in proc.info['name'].lower():
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    
                    if ('main.py worker' in cmdline or 
                        (current_dir in cmdline and 'worker' in cmdline)):
                        
                        create_time = datetime.fromtimestamp(proc.info['create_time'])
                        uptime = datetime.now() - create_time
                        
                        return {
                            'running': True,
                            'pid': proc.info['pid'],
                            'since': f"Activo {uptime.seconds//60}m"
                        }
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        
        return {'running': False, 'pid': None, 'since': 'Detenido'}
        
    except ImportError:
        return {'running': None, 'pid': None, 'since': 'psutil no instalado'}
    except Exception as e:
        return {'running': None, 'pid': None, 'since': f'Error: {str(e)[:20]}...'}

def format_size(size_bytes):
    """Formatear bytes a formato legible"""
    if size_bytes == 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"