#!/usr/bin/env python3
"""
Test CloudWatch Metrics - Probar métricas de CloudWatch

Script para generar métricas de prueba y verificar la integración
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cloudwatch_monitor import CloudWatchMonitor, DataLakeMetrics
import time
import random
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_metrics():
    """Generar métricas de prueba"""
    logger.info("🧪 Iniciando prueba de métricas CloudWatch...")
    
    # Crear monitor y métricas
    monitor = CloudWatchMonitor()
    metrics = DataLakeMetrics(monitor)
    
    # Simular procesamiento de archivos
    logger.info("📁 Simulando procesamiento de archivos...")
    for i in range(5):
        file_size = random.randint(1000, 50000000)  # 1KB - 50MB
        processing_time = random.uniform(1.0, 30.0)  # 1-30 segundos
        status = "SUCCESS" if random.random() > 0.2 else "ERROR"  # 80% éxito
        
        metrics.record_file_processed(file_size, processing_time, status)
        logger.info(f"  📄 Archivo {i+1}: {file_size} bytes, {processing_time:.1f}s, {status}")
        time.sleep(1)
    
    # Simular actividad SQS
    logger.info("📬 Simulando actividad SQS...")
    messages_received = random.randint(10, 100)
    messages_processed = random.randint(int(messages_received * 0.7), messages_received)
    
    metrics.record_sqs_activity(messages_received, messages_processed)
    logger.info(f"  📨 SQS: {messages_received} recibidos, {messages_processed} procesados")
    
    # Simular operaciones S3
    logger.info("🪣 Simulando operaciones S3...")
    operations = ['PUT', 'GET', 'DELETE', 'LIST']
    for op in operations:
        count = random.randint(1, 10)
        metrics.record_s3_operations(op, count)
        logger.info(f"  🔄 S3 {op}: {count} operaciones")
    
    # Simular actualización Glue Catalog
    logger.info("🗂️ Simulando actualización Glue Catalog...")
    tables_updated = random.randint(1, 5)
    success = random.random() > 0.1  # 90% éxito
    
    metrics.record_glue_catalog_update(tables_updated, success)
    status_text = "✅ ÉXITO" if success else "❌ ERROR"
    logger.info(f"  📊 Glue: {tables_updated} tablas, {status_text}")
    
    logger.info("✅ Prueba de métricas completada")
    logger.info("🔍 Revisa CloudWatch Console para ver las métricas")

def view_recent_metrics():
    """Ver métricas recientes"""
    logger.info("📊 Obteniendo métricas recientes...")
    
    monitor = CloudWatchMonitor()
    
    metrics_to_check = [
        'FilesProcessed',
        'FilesSuccess', 
        'FilesError',
        'SQSMessagesReceived',
        'SQSMessagesProcessed',
        'S3Operations'
    ]
    
    for metric_name in metrics_to_check:
        data = monitor.get_metrics(metric_name, hours=1)
        if data:
            latest = data[-1]
            timestamp = latest['Timestamp'].strftime('%H:%M:%S')
            value = latest.get('Sum', latest.get('Average', 0))
            logger.info(f"  📈 {metric_name}: {value} ({timestamp})")
        else:
            logger.info(f"  📉 {metric_name}: Sin datos")

def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Probar métricas CloudWatch')
    parser.add_argument('--generate', action='store_true', help='Generar métricas de prueba')
    parser.add_argument('--view', action='store_true', help='Ver métricas recientes')
    
    args = parser.parse_args()
    
    if args.generate:
        test_metrics()
    elif args.view:
        view_recent_metrics()
    else:
        # Por defecto, hacer ambos
        test_metrics()
        time.sleep(5)  # Esperar un poco
        view_recent_metrics()

if __name__ == "__main__":
    main()