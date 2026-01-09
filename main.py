"""
AWS Data Lake Control - Aplicación Principal
Orquesta el procesamiento de datos, catálogo y monitoreo
"""

import logging
import argparse
import sys
import yaml
import os
import subprocess
from pathlib import Path

# Agregar src al path
sys.path.append(str(Path(__file__).parent / "src"))

from src.datalake.sqs_worker import run_sqs_worker
from src.datalake.s3_io import read_s3_object, write_parquet_to_s3
from src.glue_catalog import GlueCatalogManager
from src.file_reader import run_read_files
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
    logger.info(" Iniciando SQS Worker...")
    config = load_config()
    
    def handle_message(message):
        logger.info(f" Procesando mensaje: {message['Body']}")
        return True
    
    run_sqs_worker(
        queue_url=config['aws']['sqs_queue_url'],
        handle_message=handle_message,
        poll_interval=config['worker']['poll_interval'],
        max_retries=config['worker']['max_retries'],
        max_empty_polls=5
    )

def run_glue_catalog():
    """Actualizar catálogo de Glue"""
    logger.info(" Actualizando catálogo de Glue...")
    config = load_config()
    catalog = GlueCatalogManager(config['glue']['database_name'])
    
    # Crear database si no existe
    catalog.create_database()
    
    # Listar tablas existentes
    tables = catalog.list_tables()
    logger.info(f" Tablas registradas: {tables}")

def run_athena_query():
    """Ejecutar consulta de ejemplo en Athena"""
    logger.info(" Ejecutando consulta en Athena...")
    
    # Ejecutar athena_query_example.py con el PYTHONPATH correcto
    env = os.environ.copy()
    env['PYTHONPATH'] = 'src'
    
    result = subprocess.run(
        [sys.executable, 'scripts/athena_query_example.py'],
        env=env,
        cwd=Path(__file__).parent
    )
    
    if result.returncode == 0:
        logger.info(" Consulta Athena ejecutada exitosamente")
    else:
        logger.error(" Error en consulta Athena")
        sys.exit(1)

def run_dashboard():
    """Ejecutar dashboard de Streamlit"""
    logger.info(" Iniciando dashboard de Streamlit...")
    
    # Ejecutar streamlit con el PYTHONPATH correcto
    env = os.environ.copy()
    env['PYTHONPATH'] = 'src'
    
    result = subprocess.run(
        [sys.executable, '-m', 'streamlit', 'run', 'dashboard/app.py'],
        env=env,
        cwd=Path(__file__).parent
    )
    
    if result.returncode != 0:
        logger.error(" Error ejecutando dashboard")
        sys.exit(1)

def run_test_pipeline():
    """Ejecutar pipeline de prueba"""
    logger.info(" Ejecutando pipeline de prueba...")
    
    # Ejecutar test_pipeline.py con el PYTHONPATH correcto
    env = os.environ.copy()
    env['PYTHONPATH'] = 'src'
    
    result = subprocess.run(
        [sys.executable, 'scripts/test_pipeline.py'],
        env=env,
        cwd=Path(__file__).parent
    )
    
    if result.returncode == 0:
        logger.info(" Pipeline de prueba ejecutado exitosamente")
    else:
        logger.error(" Error en pipeline de prueba")
        sys.exit(1)

def run_s3_sync(bucket: str, prefix: str = "", limit: int = None, date_filter: str = None, latest: int = None):
    """Sincronizar archivos con S3"""
    logger.info(f" Sincronizando con S3: {bucket}/{prefix}")
    s3 = boto3.client('s3')
    
    # Listar archivos
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get('Contents', [])
    
    # Filtrar por fecha si se especifica
    if date_filter:
        from datetime import datetime
        try:
            # Formato: YYYY-MM-DD
            filter_date = datetime.strptime(date_filter, '%Y-%m-%d').date()
            objects = [obj for obj in objects if obj['LastModified'].date() == filter_date]
            logger.info(f" Filtrado por fecha {date_filter}")
        except ValueError:
            logger.error(" Formato de fecha inválido. Use YYYY-MM-DD (ej: 2026-01-08)")
            return
    
    # Ordenar por fecha (más recientes primero) si se pide los últimos
    if latest:
        objects = sorted(objects, key=lambda x: x['LastModified'], reverse=True)
        objects = objects[:latest]
        logger.info(f" Mostrando los últimos {latest} archivos")
    
    files = [obj['Key'] for obj in objects]
    logger.info(f" Archivos encontrados: {len(files)}")
    
    # Mostrar archivos según el límite
    if limit is None:
        # Mostrar todos los archivos
        for i, obj in enumerate(objects):
            date_str = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"  - {obj['Key']} (subido: {date_str})")
    else:
        # Mostrar solo los primeros N archivos
        for i, obj in enumerate(objects[:limit]):
            date_str = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"  - {obj['Key']} (subido: {date_str})")
        if len(objects) > limit:
            logger.info(f"  ... y {len(objects) - limit} archivos más")

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description='AWS Data Lake Control')
    parser.add_argument('command', choices=['worker', 'glue', 's3-sync', 'pipeline', 'dashboard', 'athena', 'read'], 
                       help='Comando a ejecutar')
    parser.add_argument('--bucket', help='Bucket S3 para sincronización')
    parser.add_argument('--prefix', default='', help='Prefijo S3')
    parser.add_argument('--limit', type=int, help='Número máximo de archivos a mostrar (default: todos)')
    parser.add_argument('--date', help='Filtrar por fecha (formato: YYYY-MM-DD, ej: 2026-01-08)')
    parser.add_argument('--latest', type=int, help='Mostrar los N archivos más recientes')
    
    args = parser.parse_args()
    
    try:
        if args.command == 'worker':
            run_worker()
        elif args.command == 'glue':
            run_glue_catalog()
        elif args.command == 's3-sync':
            if not args.bucket:
                logger.error("--bucket es requerido para s3-sync")
                sys.exit(1)
            run_s3_sync(args.bucket, args.prefix, args.limit, args.date, args.latest)
        elif args.command == 'pipeline':
            run_test_pipeline()
        elif args.command == 'athena':
            run_athena_query()
        elif args.command == 'read':
            config = load_config()
            run_read_files(config)
        elif args.command == 'dashboard':
            run_dashboard()
            
    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
    except Exception as e:
        logger.error(f"Error en ejecución: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()