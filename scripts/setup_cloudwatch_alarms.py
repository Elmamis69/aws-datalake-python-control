#!/usr/bin/env python3
"""
Setup CloudWatch Alarms - Configurar alarmas del Data Lake

Configura alarmas automáticas para monitoreo del pipeline
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cloudwatch_monitor import CloudWatchMonitor, setup_default_alarms
import yaml
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config():
    """Cargar configuración"""
    config_path = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
    
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error cargando config: {e}")
        return {}

def main():
    """Configurar alarmas de CloudWatch"""
    logger.info("🚨 Configurando alarmas de CloudWatch...")
    
    config = load_config()
    
    # Crear monitor
    monitor = CloudWatchMonitor()
    
    # SNS topic para notificaciones (opcional)
    sns_topic = config.get('cloudwatch', {}).get('sns_topic')
    
    if sns_topic:
        logger.info(f"📧 Usando SNS topic: {sns_topic}")
    else:
        logger.warning("⚠️ No hay SNS topic configurado - alarmas sin notificaciones")
    
    # Configurar alarmas por defecto
    setup_default_alarms(monitor, sns_topic)
    
    # Alarmas adicionales específicas
    logger.info("⚙️ Configurando alarmas adicionales...")
    
    # Alarma por archivos grandes (>100MB)
    monitor.create_alarm(
        alarm_name='DataLake-LargeFiles',
        metric_name='FileSize',
        threshold=104857600,  # 100MB en bytes
        sns_topic=sns_topic
    )
    
    # Alarma por muchos mensajes SQS sin procesar
    monitor.create_alarm(
        alarm_name='DataLake-SQSBacklog',
        metric_name='SQSMessagesReceived',
        threshold=50,
        sns_topic=sns_topic
    )
    
    logger.info("✅ Alarmas configuradas correctamente")
    
    # Mostrar alarmas existentes
    alarms = monitor.get_alarms()
    logger.info(f"📊 Total de alarmas activas: {len(alarms)}")
    
    for alarm in alarms:
        state = alarm['StateValue']
        emoji = "🟢" if state == "OK" else "🔴" if state == "ALARM" else "🟡"
        logger.info(f"{emoji} {alarm['AlarmName']}: {state}")

if __name__ == "__main__":
    main()