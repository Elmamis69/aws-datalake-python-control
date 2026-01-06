import boto3
import yaml
import os
import time

# Cargar configuración desde settings.yaml
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

region = config['aws'].get('region', 'us-east-2')
database = config['glue']['database_name']
# Usa el nombre de una tabla detectada por Glue, por ejemplo year_2026
table = 'year_2026'

# Cambia el bucket de resultados si lo deseas
output_bucket = config['aws']['s3_processed_bucket']
output_prefix = 'athena-results/'
output_location = f's3://{output_bucket}/{output_prefix}'

athena = boto3.client('athena', region_name=region)

query = f"SELECT * FROM {database}.{table} LIMIT 10;"
print(f"Lanzando query: {query}")

response = athena.start_query_execution(
    QueryString=query,
    QueryExecutionContext={'Database': database},
    ResultConfiguration={'OutputLocation': output_location}
)

query_execution_id = response['QueryExecutionId']
print(f"QueryExecutionId: {query_execution_id}")

# Esperar a que termine la consulta
while True:
    result = athena.get_query_execution(QueryExecutionId=query_execution_id)
    state = result['QueryExecution']['Status']['State']
    if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        print(f"Estado final: {state}")
        break
    print(f"Estado: {state}... esperando...")
    time.sleep(2)

if state == 'SUCCEEDED':
    # Obtener resultados
    result = athena.get_query_results(QueryExecutionId=query_execution_id)
    print("Resultados:")
    for row in result['ResultSet']['Rows']:
        print([col.get('VarCharValue', '') for col in row['Data']])
else:
    print("La consulta falló o fue cancelada.")
