#!/usr/bin/env python3
"""
Script para enviar métricas de prueba adicionales
"""

import time
import random
from src.cloudwatch_monitor import CloudWatchMonitor, DataLakeMetrics

def send_continuous_metrics():
    """Envía métricas cada minuto durante 10 minutos"""
    monitor = CloudWatchMonitor()
    metrics = DataLakeMetrics(monitor)
    
    print("📊 Enviando métricas continuas...")
    
    for i in range(10):
        # Simular procesamiento de archivos
        file_size = random.randint(100000, 5000000)
        processing_time = random.uniform(10, 120)
        status = 'SUCCESS' if random.random() > 0.1 else 'ERROR'
        
        metrics.record_file_processed(file_size, processing_time, status)
        
        # Simular actividad SQS
        received = random.randint(5, 20)
        processed = received - random.randint(0, 2)
        metrics.record_sqs_activity(received, processed)
        
        # Simular operaciones S3
        metrics.record_s3_operations('PUT', random.randint(1, 5))
        metrics.record_s3_operations('GET', random.randint(2, 8))
        
        print(f"✅ Lote {i+1}/10 enviado")
        
        if i < 9:  # No esperar en la última iteración
            time.sleep(60)  # Esperar 1 minuto
    
    print("🎉 Métricas continuas enviadas. Revisa el dashboard en 2-3 minutos.")

if __name__ == "__main__":
    send_continuous_metrics()