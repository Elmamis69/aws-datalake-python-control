"""
Pipeline de Prueba End-to-End

Este script simula el flujo completo del Data Lake:
1. Genera un archivo JSONL temporal con datos de prueba
2. Lo sube al bucket S3 RAW (datos sin procesar)
3. Envía un mensaje a SQS con la ubicación del archivo
4. El worker SQS procesará automáticamente el archivo
5. Limpia el archivo temporal local

Es útil para verificar que toda la infraestructura funciona correctamente.

Uso: python scripts/test_pipeline.py
"""

import boto3
import json
import os
from datetime import datetime, timezone

# Cargar configuración desde settings.yaml
import yaml

# Cargar configuración del proyecto
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

# Extraer configuración AWS
aws_conf = config['aws']
s3_raw_bucket = aws_conf['s3_raw_bucket']      # Bucket para datos sin procesar
s3_raw_prefix = aws_conf['s3_raw_prefix']      # Prefijo/carpeta en S3
queue_url = aws_conf['sqs_queue_url']          # Cola SQS para notificaciones
region = aws_conf.get('region', 'us-east-2')  # Región AWS

print(f"Configuración cargada - Bucket: {s3_raw_bucket}, Cola: {queue_url}")

# PASO 1: Crear archivo JSONL temporal con datos de prueba
print("\n📄 Paso 1: Generando archivo de prueba...")
data = [
    {"event_time": datetime.now(timezone.utc).isoformat(), "user_id": 1, "action": "login"},
    {"event_time": datetime.now(timezone.utc).isoformat(), "user_id": 2, "action": "logout"}
]

# Crear archivo JSONL (JSON Lines - un JSON por línea)
jsonl_path = "test.jsonl"
with open(jsonl_path, 'w') as f:
    for row in data:
        f.write(json.dumps(row) + '\n')
        
print(f"✅ Archivo creado: {jsonl_path} con {len(data)} registros")

# PASO 2: Subir archivo al bucket S3 RAW
print("\n📤 Paso 2: Subiendo archivo a S3...")
# Generar nombre único con timestamp para evitar conflictos
s3_key = f"{s3_raw_prefix}test_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl"

s3 = boto3.client('s3', region_name=region)
s3.upload_file(jsonl_path, s3_raw_bucket, s3_key)
print(f"✅ Archivo subido a: s3://{s3_raw_bucket}/{s3_key}")

# PASO 3: Notificar al worker vía SQS
print("\n📨 Paso 3: Enviando notificación a SQS...")
# El worker SQS procesará automáticamente el archivo cuando reciba este mensaje
sqs = boto3.client('sqs', region_name=region)
sqs.send_message(QueueUrl=queue_url, MessageBody=s3_key)
print(f"✅ Mensaje enviado a SQS - El worker procesará el archivo automáticamente")
print(f"   Contenido del mensaje: {s3_key}")

# PASO 4: Limpiar archivo temporal local
print("\n🧹 Paso 4: Limpiando archivos temporales...")
os.remove(jsonl_path)
print(f"✅ Archivo temporal eliminado: {jsonl_path}")

print("\n🎉 Pipeline de prueba completado exitosamente!")
print("\n🔍 Próximos pasos:")
print("   1. El worker SQS procesará el archivo automáticamente")
print("   2. Los datos se convertirán a formato Parquet")
print("   3. Se actualizará el catálogo de Glue")
print("   4. Podrás consultar los datos en Athena")
print("\n📊 Para monitorear: python main.py dashboard")
