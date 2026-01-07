"""
run_worker.py
Script de entrada para lanzar el worker de SQS y procesar archivos S3.
"""

import os
import yaml
import logging
from datalake.aws_session import get_boto3_session
from datalake.sqs_worker import run_sqs_worker

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')

def load_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def main():

    # Configurar logging

    # Crear carpeta de logs si no existe
    os.makedirs("logs", exist_ok=True)

    # Configurar logging para consola y archivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/worker.log", encoding="utf-8")
        ]
    )
    logger = logging.getLogger("run_worker")

    config = load_config(CONFIG_PATH)
    aws_conf = config['aws']
    worker_conf = config['worker']

    session = get_boto3_session(profile=aws_conf.get('profile'), region=aws_conf.get('region'))
    queue_url = aws_conf['sqs_queue_url']

    from datalake.s3_io import read_jsonl_from_s3, write_parquet_to_s3
    from datalake.transform import transform_jsonl_to_parquet
    import json
    glue_conf = config.get('glue', {})
    import boto3
    import time

    s3_raw_bucket = aws_conf['s3_raw_bucket']
    s3_raw_prefix = aws_conf['s3_raw_prefix']
    s3_processed_bucket = aws_conf['s3_processed_bucket']
    s3_processed_prefix = aws_conf['s3_processed_prefix']
    processing_conf = config.get('processing', {})

    processed_files = set()  # Evitar procesar el mismo archivo múltiples veces
    
    def handle_message(msg):
        # El mensaje de S3 Event Notification puede estar anidado en 'Body'
        body = msg['Body']
        try:
            event = json.loads(body)
            # Si el mensaje viene de S3 Event, buscar el registro
            if 'Records' in event:
                record = event['Records'][0]
                s3_info = record['s3']
                bucket = s3_info['bucket']['name']
                key = s3_info['object']['key']
            else:
                # Mensaje custom, usar configuración
                bucket = s3_raw_bucket
                key = body.strip()
        except Exception:
            # Si no es JSON, asumir que el body es el key
            bucket = s3_raw_bucket
            key = body.strip()

        # Evitar procesar el mismo archivo múltiples veces
        file_id = f"{bucket}/{key}"
        if file_id in processed_files:
            logger.info(f"Archivo ya procesado, omitiendo: s3://{bucket}/{key}")
            return True
        
        processed_files.add(file_id)
        logger.info(f"Procesando archivo S3: s3://{bucket}/{key}")
        
        try:
            df = read_jsonl_from_s3(bucket, key, session)
            result = transform_jsonl_to_parquet(
                df,
                date_field=processing_conf.get('date_field', 'event_time'),
                partition_by_date=processing_conf.get('partition_by_date', True)
            )
            out_df = result['df']
            partition_path = result['partition_path']
            # Construir el key de salida
            filename = key.split('/')[-1].replace('.jsonl', '.parquet')
            out_key = f"{s3_processed_prefix}{partition_path}{filename}"
            write_parquet_to_s3(out_df, s3_processed_bucket, out_key, session)
            logger.info(f"Archivo procesado y guardado en s3://{s3_processed_bucket}/{out_key}")

            logger.info("Procesamiento completado exitosamente.")
            return True
        except Exception as e:
            logger.error(f"Error procesando archivo: {e}")
            processed_files.discard(file_id)  # Remover de procesados si falló
            return False

    # Configuración para terminar después de procesar mensajes
    max_empty_polls = worker_conf.get('max_empty_polls', 3)  # Terminar después de 3 polls vacíos
    
    run_sqs_worker(
        queue_url=queue_url,
        session=session,
        handle_message=handle_message,
        poll_interval=worker_conf.get('poll_interval', 10),
        max_retries=worker_conf.get('max_retries', 3),
        max_empty_polls=max_empty_polls
    )

if __name__ == "__main__":
    main()
