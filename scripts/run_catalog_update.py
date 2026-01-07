"""
run_catalog_update.py
Script para actualizar el catálogo de Glue y ejecutar consultas Athena.
"""

import os
import yaml
import logging
import boto3
import time
from datalake.aws_session import get_boto3_session

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')

def load_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("catalog_update")
    
    config = load_config(CONFIG_PATH)
    aws_conf = config['aws']
    glue_conf = config.get('glue', {})
    
    session = get_boto3_session(profile=aws_conf.get('profile'), region=aws_conf.get('region'))
    
    # Lanzar Glue Crawler
    glue_client = boto3.client('glue', region_name=aws_conf.get('region', 'us-east-2'))
    crawler_name = glue_conf.get('crawler_name', 'datalake-processed-crawler')
    
    try:
        glue_client.start_crawler(Name=crawler_name)
        logger.info(f"Glue Crawler '{crawler_name}' lanzado.")
    except glue_client.exceptions.CrawlerRunningException:
        logger.info(f"Glue Crawler '{crawler_name}' ya está en ejecución.")
    except Exception as e:
        logger.error(f"Error al lanzar Glue Crawler: {e}")
        return
    
    # Esperar a que termine
    logger.info("Esperando a que termine el Glue Crawler...")
    while True:
        crawler = glue_client.get_crawler(Name=crawler_name)
        state = crawler['Crawler']['State']
        if state == 'READY':
            logger.info("Glue Crawler terminó.")
            break
        logger.info(f"Estado: {state}... esperando...")
        time.sleep(5)
    
    # Ejecutar consulta Athena
    athena = boto3.client('athena', region_name=aws_conf.get('region', 'us-east-2'))
    database = glue_conf.get('database_name', 'datalake_processed_db')
    table = 'year_2026'
    output_bucket = aws_conf['s3_processed_bucket']
    output_location = f's3://{output_bucket}/athena-results/'
    query = f"SELECT * FROM {database}.{table} LIMIT 10;"
    
    logger.info(f"Ejecutando consulta: {query}")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    
    query_execution_id = response['QueryExecutionId']
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            logger.info(f"Consulta terminó: {state}")
            break
        time.sleep(2)
    
    if state == 'SUCCEEDED':
        result = athena.get_query_results(QueryExecutionId=query_execution_id)
        logger.info("Resultados:")
        for row in result['ResultSet']['Rows']:
            logger.info([col.get('VarCharValue', '') for col in row['Data']])

if __name__ == "__main__":
    main()