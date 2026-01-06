import boto3
import yaml
import os

# Cargar configuración desde settings.yaml
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

glue_conf = config.get('glue', {})
crawler_name = glue_conf.get('crawler_name', 'datalake-processed-crawler')
region = config['aws'].get('region', 'us-east-2')

# Lanzar el Glue Crawler
client = boto3.client('glue', region_name=region)

try:
    response = client.start_crawler(Name=crawler_name)
    print(f"Crawler '{crawler_name}' lanzado correctamente.")
except client.exceptions.CrawlerRunningException:
    print(f"El crawler '{crawler_name}' ya está en ejecución.")
except client.exceptions.EntityNotFoundException:
    print(f"El crawler '{crawler_name}' no existe. Crea el crawler en la consola de AWS Glue primero.")
except Exception as e:
    print(f"Error al lanzar el crawler: {e}")
