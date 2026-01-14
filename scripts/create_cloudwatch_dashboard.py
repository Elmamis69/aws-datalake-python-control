"""
Crear Dashboard Nativo de CloudWatch
Este script crea un dashboard en la consola de AWS CloudWatch (GRATIS)
"""

import boto3
import json
from src.cloudwatch_monitor import CloudWatchMonitor

def create_native_cloudwatch_dashboard():
    """Crear dashboard nativo en CloudWatch Console"""
    
    cloudwatch = boto3.client('cloudwatch')
    
    # Configuración del dashboard
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "x": 0,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["DataLake/Pipeline", "FilesProcessed"],
                        [".", "FilesSuccess"],
                        [".", "FilesError"]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": "us-east-1",
                    "title": "📁 Procesamiento de Archivos",
                    "period": 300
                }
            },
            {
                "type": "metric",
                "x": 12,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["DataLake/Pipeline", "ProcessingTime"]
                    ],
                    "view": "timeSeries",
                    "region": "us-east-1",
                    "title": "⏱️ Tiempo de Procesamiento",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "min": 0
                        }
                    }
                }
            },
            {
                "type": "metric",
                "x": 0,
                "y": 6,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["DataLake/Pipeline", "SQSMessagesReceived"],
                        [".", "SQSMessagesProcessed"]
                    ],
                    "view": "timeSeries",
                    "region": "us-east-1",
                    "title": "📬 Actividad SQS",
                    "period": 300
                }
            },
            {
                "type": "metric",
                "x": 12,
                "y": 6,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["DataLake/Pipeline", "SQSSuccessRate"]
                    ],
                    "view": "timeSeries",
                    "region": "us-east-1",
                    "title": "📊 Tasa de Éxito SQS (%)",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "min": 0,
                            "max": 100
                        }
                    }
                }
            },
            {
                "type": "metric",
                "x": 0,
                "y": 12,
                "width": 24,
                "height": 6,
                "properties": {
                    "metrics": [
                        ["DataLake/Pipeline", "S3Operations", "Operation", "PUT"],
                        ["...", "GET"],
                        ["...", "DELETE"]
                    ],
                    "view": "timeSeries",
                    "region": "us-east-1",
                    "title": "🗂️ Operaciones S3",
                    "period": 300
                }
            }
        ]
    }
    
    try:
        # Crear el dashboard
        response = cloudwatch.put_dashboard(
            DashboardName='DataLake-Monitor',
            DashboardBody=json.dumps(dashboard_body)
        )
        
        print("✅ Dashboard de CloudWatch creado exitosamente!")
        print(f"🔗 URL: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=DataLake-Monitor")
        print("\n📊 El dashboard incluye:")
        print("   • Procesamiento de archivos")
        print("   • Tiempo de procesamiento")
        print("   • Actividad SQS")
        print("   • Tasa de éxito")
        print("   • Operaciones S3")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creando dashboard: {e}")
        return False

def setup_sample_metrics():
    """Enviar métricas de ejemplo para que el dashboard tenga datos"""
    
    print("\n📈 Enviando métricas de ejemplo...")
    
    monitor = CloudWatchMonitor()
    
    # Métricas de ejemplo
    monitor.send_metric('FilesProcessed', 10)
    monitor.send_metric('FilesSuccess', 8)
    monitor.send_metric('FilesError', 2)
    monitor.send_metric('ProcessingTime', 45.5, 'Seconds')
    monitor.send_metric('SQSMessagesReceived', 15)
    monitor.send_metric('SQSMessagesProcessed', 12)
    monitor.send_metric('SQSSuccessRate', 80, 'Percent')
    monitor.send_metric('S3Operations', 5, dimensions={'Operation': 'PUT'})
    monitor.send_metric('S3Operations', 3, dimensions={'Operation': 'GET'})
    
    print("✅ Métricas de ejemplo enviadas")

if __name__ == "__main__":
    print("🏗️ Creando Dashboard Nativo de CloudWatch...")
    print("=" * 50)
    
    # Crear dashboard
    if create_native_cloudwatch_dashboard():
        
        # Enviar métricas de ejemplo
        setup_sample_metrics()
        
        print("\n🎉 ¡Dashboard listo!")
        print("💡 Ve a la consola de AWS CloudWatch para verlo")
        print("⏰ Las métricas aparecerán en unos minutos")
    
    else:
        print("❌ No se pudo crear el dashboard")