"""
CloudWatch Monitor - Sistema de métricas y alertas

Integra CloudWatch con el Data Lake para:
- Enviar métricas personalizadas
- Crear alarmas automáticas
- Monitorear rendimiento del pipeline
- Alertas por email/SNS
"""

import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json

logger = logging.getLogger(__name__)

class CloudWatchMonitor:
    """Monitor de CloudWatch para el Data Lake"""
    
    def __init__(self, namespace: str = "DataLake/Pipeline"):
        self.cloudwatch = boto3.client('cloudwatch')
        self.sns = boto3.client('sns')
        self.namespace = namespace
        
    def send_metric(self, metric_name: str, value: float, unit: str = 'Count', dimensions: Dict = None):
        """Enviar métrica personalizada a CloudWatch"""
        try:
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.utcnow()
            }
            
            if dimensions:
                metric_data['Dimensions'] = [
                    {'Name': k, 'Value': v} for k, v in dimensions.items()
                ]
            
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[metric_data]
            )
            
            logger.info(f"✅ Métrica enviada: {metric_name} = {value}")
            
        except Exception as e:
            logger.error(f"❌ Error enviando métrica {metric_name}: {e}")
    
    def get_metrics(self, metric_name: str, hours: int = 24) -> List[Dict]:
        """Obtener métricas de CloudWatch"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)
            
            response = self.cloudwatch.get_metric_statistics(
                Namespace=self.namespace,
                MetricName=metric_name,
                StartTime=start_time,
                EndTime=end_time,
                Period=300,  # 5 minutos
                Statistics=['Sum', 'Average', 'Maximum']
            )
            
            return sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo métricas {metric_name}: {e}")
            return []
    
    def create_alarm(self, alarm_name: str, metric_name: str, threshold: float, 
                    comparison: str = 'GreaterThanThreshold', sns_topic: str = None):
        """Crear alarma de CloudWatch"""
        try:
            alarm_config = {
                'AlarmName': alarm_name,
                'ComparisonOperator': comparison,
                'EvaluationPeriods': 2,
                'MetricName': metric_name,
                'Namespace': self.namespace,
                'Period': 300,
                'Statistic': 'Sum',
                'Threshold': threshold,
                'ActionsEnabled': True,
                'AlarmDescription': f'Alarma para {metric_name}',
                'Unit': 'Count'
            }
            
            if sns_topic:
                alarm_config['AlarmActions'] = [sns_topic]
                alarm_config['OKActions'] = [sns_topic]
            
            self.cloudwatch.put_metric_alarm(**alarm_config)
            logger.info(f"✅ Alarma creada: {alarm_name}")
            
        except Exception as e:
            logger.error(f"❌ Error creando alarma {alarm_name}: {e}")
    
    def get_alarms(self) -> List[Dict]:
        """Obtener estado de todas las alarmas"""
        try:
            response = self.cloudwatch.describe_alarms()
            return response['MetricAlarms']
        except Exception as e:
            logger.error(f"❌ Error obteniendo alarmas: {e}")
            return []

class DataLakeMetrics:
    """Métricas específicas del Data Lake"""
    
    def __init__(self, monitor: CloudWatchMonitor):
        self.monitor = monitor
    
    def record_file_processed(self, file_size: int, processing_time: float, status: str):
        """Registrar procesamiento de archivo"""
        # Métricas básicas
        self.monitor.send_metric('FilesProcessed', 1)
        self.monitor.send_metric('FileSize', file_size, 'Bytes')
        self.monitor.send_metric('ProcessingTime', processing_time, 'Seconds')
        
        # Métricas por estado
        dimensions = {'Status': status}
        self.monitor.send_metric('ProcessingStatus', 1, dimensions=dimensions)
        
        if status == 'SUCCESS':
            self.monitor.send_metric('FilesSuccess', 1)
        else:
            self.monitor.send_metric('FilesError', 1)
    
    def record_sqs_activity(self, messages_received: int, messages_processed: int):
        """Registrar actividad de SQS"""
        self.monitor.send_metric('SQSMessagesReceived', messages_received)
        self.monitor.send_metric('SQSMessagesProcessed', messages_processed)
        
        if messages_received > 0:
            success_rate = (messages_processed / messages_received) * 100
            self.monitor.send_metric('SQSSuccessRate', success_rate, 'Percent')
    
    def record_s3_operations(self, operation: str, count: int = 1):
        """Registrar operaciones S3"""
        dimensions = {'Operation': operation}
        self.monitor.send_metric('S3Operations', count, dimensions=dimensions)
    
    def record_glue_catalog_update(self, tables_updated: int, success: bool):
        """Registrar actualización del catálogo Glue"""
        self.monitor.send_metric('GlueTablesUpdated', tables_updated)
        
        if success:
            self.monitor.send_metric('GlueCatalogSuccess', 1)
        else:
            self.monitor.send_metric('GlueCatalogError', 1)

def setup_default_alarms(monitor: CloudWatchMonitor, sns_topic: str = None):
    """Configurar alarmas por defecto del Data Lake"""
    
    # Alarma por errores de procesamiento
    monitor.create_alarm(
        alarm_name='DataLake-ProcessingErrors',
        metric_name='FilesError',
        threshold=5,
        sns_topic=sns_topic
    )
    
    # Alarma por tiempo de procesamiento alto
    monitor.create_alarm(
        alarm_name='DataLake-SlowProcessing',
        metric_name='ProcessingTime',
        threshold=300,  # 5 minutos
        sns_topic=sns_topic
    )
    
    # Alarma por baja tasa de éxito en SQS
    monitor.create_alarm(
        alarm_name='DataLake-LowSQSSuccess',
        metric_name='SQSSuccessRate',
        threshold=80,
        comparison='LessThanThreshold',
        sns_topic=sns_topic
    )
    
    logger.info("✅ Alarmas por defecto configuradas")