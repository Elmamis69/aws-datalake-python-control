#!/usr/bin/env python3
"""
Script para crear dashboard de CloudWatch para el Data Lake
Ejecutar: python create_dashboard.py
"""

import boto3
import json
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_cloudwatch_dashboard():
    """Crear dashboard de CloudWatch para métricas del Data Lake"""
    
    try:
        cloudwatch = boto3.client('cloudwatch')
        
        # Configuración del dashboard
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "x": 0, "y": 0,
                    "width": 12, "height": 6,
                    "properties": {
                        "metrics": [
                            ["DataLake/Pipeline", "FilesProcessed"],
                            [".", "FilesSuccess"],
                            [".", "FilesError"]
                        ],
                        "period": 300,
                        "stat": "Sum",
                        "region": "us-east-1",
                        "title": "Archivos Procesados"
                    }
                },
                {
                    "type": "metric",
                    "x": 12, "y": 0,
                    "width": 12, "height": 6,
                    "properties": {
                        "metrics": [
                            ["DataLake/Pipeline", "ProcessingTime"]
                        ],
                        "period": 300,
                        "stat": "Average",
                        "region": "us-east-1",
                        "title": "Tiempo de Procesamiento"
                    }
                },
                {
                    "type": "metric",
                    "x": 0, "y": 6,
                    "width": 12, "height": 6,
                    "properties": {
                        "metrics": [
                            ["DataLake/Pipeline", "SQSMessagesReceived"],
                            [".", "SQSMessagesProcessed"]
                        ],
                        "period": 300,
                        "stat": "Sum",
                        "region": "us-east-1",
                        "title": "Actividad SQS"
                    }
                },
                {
                    "type": "metric",
                    "x": 12, "y": 6,
                    "width": 12, "height": 6,
                    "properties": {
                        "metrics": [
                            ["DataLake/Pipeline", "SQSSuccessRate"]
                        ],
                        "period": 300,
                        "stat": "Average",
                        "region": "us-east-1",
                        "title": "Tasa de Éxito SQS (%)"
                    }
                }
            ]
        }
        
        # Crear el dashboard
        response = cloudwatch.put_dashboard(
            DashboardName='DataLake-Monitor',
            DashboardBody=json.dumps(dashboard_body)
        )
        
        logger.info("✅ Dashboard 'DataLake-Monitor' creado exitosamente")
        logger.info(f"URL: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=DataLake-Monitor")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creando dashboard: {e}")
        return False

def create_sample_metrics():
    """Crear métricas de ejemplo para probar el dashboard"""
    
    try:
        from src.cloudwatch_monitor import CloudWatchMonitor, DataLakeMetrics
        
        monitor = CloudWatchMonitor()
        metrics = DataLakeMetrics(monitor)
        
        # Enviar métricas de ejemplo
        logger.info("📊 Enviando métricas de ejemplo...")
        
        metrics.record_file_processed(1024000, 45.5, 'SUCCESS')
        metrics.record_file_processed(2048000, 67.2, 'SUCCESS')
        metrics.record_file_processed(512000, 23.1, 'ERROR')
        
        metrics.record_sqs_activity(10, 9)
        metrics.record_sqs_activity(15, 14)
        
        metrics.record_s3_operations('PUT', 5)
        metrics.record_s3_operations('GET', 12)
        
        logger.info("✅ Métricas de ejemplo enviadas")
        
    except Exception as e:
        logger.error(f"❌ Error enviando métricas: {e}")

if __name__ == "__main__":
    print("🚀 Creando dashboard de CloudWatch...")
    
    # Crear dashboard
    if create_cloudwatch_dashboard():
        print("\n📈 Dashboard creado exitosamente!")
        
        # Preguntar si quiere métricas de ejemplo
        response = input("\n¿Quieres enviar métricas de ejemplo para probar el dashboard? (y/n): ")
        if response.lower() in ['y', 'yes', 's', 'si']:
            create_sample_metrics()
            print("\n✅ ¡Listo! Revisa tu dashboard en CloudWatch")
        else:
            print("\n💡 Puedes enviar métricas usando el CloudWatchMonitor en tu código")
    else:
        print("\n❌ Error creando el dashboard")