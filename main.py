"""
AWS Data Lake Control - Aplicación Principal
Orquesta el procesamiento de datos, catálogo y monitoreo
"""

import logging
import argparse
import sys
import yaml
from pathlib import Path

# Agregar src al path
sys.path.append(str(Path(__file__).parent / "src"))

from src.datalake.sqs_worker import run_sqs_worker
from src.datalake.s3_io import read_s3_object, write_parquet_to_s3
from src.glue_catalog import GlueCatalogManager
import boto3

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/main.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def load_config():
    """Cargar configuración desde settings.yaml"""
    config_path = Path(__file__).parent / "config" / "settings.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def run_worker():
    """Ejecutar el worker SQS para procesamiento automático"""
    logger.info("Iniciando SQS Worker...")
    config = load_config()
    
    def handle_message(message):
        logger.info(f"Procesando mensaje: {message['Body']}")
        return True
    
    run_sqs_worker(
        queue_url=config['aws']['sqs_queue_url'],
        handle_message=handle_message,
        poll_interval=config['worker']['poll_interval'],
        max_retries=config['worker']['max_retries'],
        max_empty_polls=5
    )

def run_catalog_update():
    """Actualizar catálogo de Glue"""
    logger.info("Actualizando catálogo de Glue...")
    config = load_config()
    catalog = GlueCatalogManager(config['glue']['database_name'])
    
    # Crear database si no existe
    catalog.create_database()
    
    # Listar tablas existentes
    tables = catalog.list_tables()
    logger.info(f"Tablas registradas: {tables}")

def run_s3_sync(bucket: str, prefix: str = ""):
    """Sincronizar archivos con S3"""
    logger.info(f"Sincronizando con S3: {bucket}/{prefix}")
    s3 = boto3.client('s3')
    
    # Listar archivos procesados
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', [])]
    logger.info(f"Archivos encontrados: {len(files)}")
    
    for file in files[:5]:  # Mostrar primeros 5
        logger.info(f"  - {file}")

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description='AWS Data Lake Control')
    parser.add_argument('command', choices=['worker', 'catalog', 's3-sync'], 
                       help='Comando a ejecutar')
    parser.add_argument('--bucket', help='Bucket S3 para sincronización')
    parser.add_argument('--prefix', default='', help='Prefijo S3')
    
    args = parser.parse_args()
    
    try:
        if args.command == 'worker':
            run_worker()
        elif args.command == 'catalog':
            run_catalog_update()
        elif args.command == 's3-sync':
            if not args.bucket:
                logger.error("--bucket es requerido para s3-sync")
                sys.exit(1)
            run_s3_sync(args.bucket, args.prefix)
            
    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
    except Exception as e:
        logger.error(f"Error en ejecución: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()