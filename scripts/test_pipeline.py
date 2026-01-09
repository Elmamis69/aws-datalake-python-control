# Script para pruebas end-to-end del pipeline
# 1. Genera un archivo JSONL de prueba
# 2. Lo sube a S3 RAW
# 3. Envía un mensaje a SQS con el key del archivo

import boto3
import json
import os
from datetime import datetime, timezone

# Cargar configuración desde settings.yaml
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

aws_conf = config['aws']
s3_raw_bucket = aws_conf['s3_raw_bucket']
s3_raw_prefix = aws_conf['s3_raw_prefix']
queue_url = aws_conf['sqs_queue_url']
region = aws_conf.get('region', 'us-east-2')

# 1. Crear archivo JSONL de prueba
data = [
    {"event_time": datetime.now(timezone.utc).isoformat(), "user_id": 1, "action": "login"},
    {"event_time": datetime.now(timezone.utc).isoformat(), "user_id": 2, "action": "logout"}
]
jsonl_path = "test.jsonl"
with open(jsonl_path, 'w') as f:
    for row in data:
        f.write(json.dumps(row) + '\n')

# 2. Subir archivo a S3 RAW
s3_key = f"{s3_raw_prefix}test_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl"
s3 = boto3.client('s3', region_name=region)
s3.upload_file(jsonl_path, s3_raw_bucket, s3_key)
print(f"Archivo subido a s3://{s3_raw_bucket}/{s3_key}")

# 3. Enviar mensaje a SQS (solo el key, el worker lo soporta)
sqs = boto3.client('sqs', region_name=region)
sqs.send_message(QueueUrl=queue_url, MessageBody=s3_key)
print(f"Mensaje enviado a SQS con key: {s3_key}")

# Limpieza local
os.remove(jsonl_path)
print("Archivo local de prueba eliminado.")
