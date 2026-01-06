import boto3
import yaml
import os

# Cargar configuración desde settings.yaml
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

aws_conf = config['aws']
glue_conf = config.get('glue', {})
crawler_name = glue_conf.get('crawler_name', 'datalake-processed-crawler')
region = aws_conf.get('region', 'us-east-2')
s3_target = aws_conf['s3_processed_bucket'] + '/' + aws_conf['s3_processed_prefix']
database_name = glue_conf.get('database_name', 'datalake_processed_db')
role_arn = glue_conf.get('role_arn', '<TU_GLUE_ROLE_ARN>')  # Debes poner el ARN del rol de Glue aquí

client = boto3.client('glue', region_name=region)

# 1. Crear la base de datos si no existe
try:
    client.create_database(DatabaseInput={'Name': database_name})
    print(f"Base de datos '{database_name}' creada.")
except client.exceptions.AlreadyExistsException:
    print(f"La base de datos '{database_name}' ya existe.")

# 2. Crear el crawler si no existe
try:
    client.create_crawler(
        Name=crawler_name,
        Role=role_arn,
        DatabaseName=database_name,
        Targets={
            'S3Targets': [{'Path': f's3://{s3_target}'}]
        },
        TablePrefix='',
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        }
    )
    print(f"Crawler '{crawler_name}' creado.")
except client.exceptions.AlreadyExistsException:
    print(f"El crawler '{crawler_name}' ya existe.")

# 3. Lanzar el crawler
try:
    client.start_crawler(Name=crawler_name)
    print(f"Crawler '{crawler_name}' lanzado correctamente.")
except client.exceptions.CrawlerRunningException:
    print(f"El crawler '{crawler_name}' ya está en ejecución.")
except Exception as e:
    print(f"Error al lanzar el crawler: {e}")
