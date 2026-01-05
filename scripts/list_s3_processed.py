import boto3
import yaml
import os

# Cargar configuración desde settings.yaml
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

aws_conf = config['aws']
bucket = aws_conf['s3_processed_bucket']
prefix = aws_conf['s3_processed_prefix'] + 'year=2026/month=01/day=05/'  # Puedes cambiar la fecha según lo que quieras buscar

s3 = boto3.client('s3', region_name=aws_conf.get('region', 'us-east-2'))

response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

if 'Contents' in response:
    print('Archivos encontrados en la ruta:')
    for obj in response['Contents']:
        print(obj['Key'])
else:
    print('No se encontraron archivos en esa ruta.')
