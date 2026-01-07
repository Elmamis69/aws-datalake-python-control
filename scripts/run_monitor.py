"""
run_monitor.py
Script para monitorear el estado del data lake desde terminal.
"""

import os
import sys
import yaml
import boto3
import argparse
from datetime import datetime, timedelta
from datalake.aws_session import get_boto3_session

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')

def load_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

class DataLakeMonitor:
    def __init__(self, config):
        self.config = config
        aws_conf = config['aws']
        self.session = get_boto3_session(
            profile=aws_conf.get('profile'), 
            region=aws_conf.get('region')
        )
        self.s3 = self.session.client('s3')
        self.sqs = self.session.client('sqs')
        self.cloudwatch = self.session.client('cloudwatch')
        
    def count_s3_objects(self, bucket, prefix):
        """Contar objetos en S3"""
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            count = 0
            total_size = 0
            for page in pages:
                if 'Contents' in page:
                    count += len(page['Contents'])
                    total_size += sum(obj['Size'] for obj in page['Contents'])
            return count, total_size
        except Exception as e:
            return 0, 0
    
    def get_sqs_message_count(self, queue_url):
        """Obtener número de mensajes en cola SQS"""
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            return int(response['Attributes']['ApproximateNumberOfMessages'])
        except Exception:
            return 0
    
    def get_recent_logs(self, hours=24):
        """Obtener errores recientes de los logs"""
        try:
            log_file = "logs/worker.log"
            if not os.path.exists(log_file):
                return []
            
            errors = []
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if 'ERROR' in line:
                        # Extraer timestamp del log
                        try:
                            timestamp_str = line.split(' ')[0] + ' ' + line.split(' ')[1]
                            log_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                            if log_time > cutoff_time:
                                errors.append(line.strip())
                        except:
                            errors.append(line.strip())
            return errors
        except Exception:
            return []
    
    def show_status(self):
        """Mostrar estado general del data lake"""
        print("=" * 50)
        print("🏗️  DATA LAKE STATUS")
        print("=" * 50)
        
        aws_conf = self.config['aws']
        
        # Archivos en raw
        raw_count, raw_size = self.count_s3_objects(
            aws_conf['s3_raw_bucket'], 
            aws_conf['s3_raw_prefix']
        )
        
        # Archivos procesados
        processed_count, processed_size = self.count_s3_objects(
            aws_conf['s3_processed_bucket'], 
            aws_conf['s3_processed_prefix']
        )
        
        # Mensajes en cola
        queue_messages = self.get_sqs_message_count(aws_conf['sqs_queue_url'])
        
        # Errores recientes
        recent_errors = self.get_recent_logs(24)
        
        print(f"📁 Archivos RAW:        {raw_count:>6} ({self.format_size(raw_size)})")
        print(f"✅ Archivos PROCESADOS: {processed_count:>6} ({self.format_size(processed_size)})")
        print(f"📬 Mensajes en cola:    {queue_messages:>6}")
        print(f"🔴 Errores (24h):       {len(recent_errors):>6}")
        print(f"⏰ Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if queue_messages > 0:
            print(f"\n⚠️  Hay {queue_messages} mensajes pendientes en la cola")
        
        if recent_errors:
            print(f"\n🚨 Errores recientes encontrados:")
            for error in recent_errors[-3:]:  # Mostrar últimos 3
                print(f"   {error}")
    
    def show_errors(self, hours=24):
        """Mostrar errores detallados"""
        print("=" * 50)
        print(f"🔴 ERRORES ÚLTIMAS {hours} HORAS")
        print("=" * 50)
        
        errors = self.get_recent_logs(hours)
        
        if not errors:
            print("✅ No se encontraron errores")
            return
        
        for i, error in enumerate(errors, 1):
            print(f"{i:>3}. {error}")
    
    def show_queue_details(self):
        """Mostrar detalles de la cola SQS"""
        print("=" * 50)
        print("📬 DETALLES DE COLA SQS")
        print("=" * 50)
        
        queue_url = self.config['aws']['sqs_queue_url']
        
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )
            attrs = response['Attributes']
            
            print(f"Cola: {queue_url.split('/')[-1]}")
            print(f"Mensajes disponibles: {attrs.get('ApproximateNumberOfMessages', 0)}")
            print(f"Mensajes en vuelo: {attrs.get('ApproximateNumberOfMessagesNotVisible', 0)}")
            print(f"Mensajes en DLQ: {attrs.get('ApproximateNumberOfMessagesDelayed', 0)}")
            
        except Exception as e:
            print(f"Error obteniendo detalles: {e}")
    
    def export_metrics(self, format='json'):
        """Exportar métricas para herramientas externas"""
        aws_conf = self.config['aws']
        
        raw_count, raw_size = self.count_s3_objects(
            aws_conf['s3_raw_bucket'], 
            aws_conf['s3_raw_prefix']
        )
        
        processed_count, processed_size = self.count_s3_objects(
            aws_conf['s3_processed_bucket'], 
            aws_conf['s3_processed_prefix']
        )
        
        queue_messages = self.get_sqs_message_count(aws_conf['sqs_queue_url'])
        recent_errors = len(self.get_recent_logs(24))
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'raw_files': raw_count,
            'raw_size_bytes': raw_size,
            'processed_files': processed_count,
            'processed_size_bytes': processed_size,
            'queue_messages': queue_messages,
            'errors_24h': recent_errors
        }
        
        if format == 'json':
            import json
            print(json.dumps(metrics, indent=2))
        elif format == 'csv':
            print(','.join(metrics.keys()))
            print(','.join(str(v) for v in metrics.values()))
    
    def format_size(self, size_bytes):
        """Formatear tamaño en bytes a formato legible"""
        if size_bytes == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

def main():
    parser = argparse.ArgumentParser(description='Monitor del Data Lake')
    parser.add_argument('command', choices=['status', 'errors', 'queue', 'export'], 
                       help='Comando a ejecutar')
    parser.add_argument('--hours', type=int, default=24, 
                       help='Horas hacia atrás para errores (default: 24)')
    parser.add_argument('--format', choices=['json', 'csv'], default='json',
                       help='Formato de exportación (default: json)')
    
    args = parser.parse_args()
    
    config = load_config(CONFIG_PATH)
    monitor = DataLakeMonitor(config)
    
    if args.command == 'status':
        monitor.show_status()
    elif args.command == 'errors':
        monitor.show_errors(args.hours)
    elif args.command == 'queue':
        monitor.show_queue_details()
    elif args.command == 'export':
        monitor.export_metrics(args.format)

if __name__ == "__main__":
    main()