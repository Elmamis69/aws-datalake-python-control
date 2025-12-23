"""
sqs_worker.py
Worker básico para consumir mensajes de SQS y procesar archivos S3.
"""
import time
import logging
import boto3
from typing import Callable, Optional

logger = logging.getLogger("sqs_worker")
logging.basicConfig(level=logging.INFO)

def run_sqs_worker(
    queue_url: str,
    session: Optional[boto3.Session] = None,
    handle_message: Optional[Callable[[dict], bool]] = None,
    poll_interval: int = 10,
    max_retries: int = 3
):
    """
    Worker principal para consumir mensajes de SQS y procesar eventos.
    Args:
        queue_url (str): URL de la cola SQS.
        session (boto3.Session, optional): Sesión boto3.
        handle_message (callable, optional): Función que procesa el mensaje. Debe retornar True si fue exitoso.
        poll_interval (int): Segundos entre polls.
        max_retries (int): Reintentos por mensaje fallido.
    """
    sqs = (session or boto3).client('sqs')
    logger.info(f"Iniciando worker SQS en {queue_url}")
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )
        messages = resp.get('Messages', [])
        if not messages:
            time.sleep(poll_interval)
            continue
        for msg in messages:
            receipt = msg['ReceiptHandle']
            body = msg['Body']
            success = False
            for attempt in range(1, max_retries+1):
                try:
                    if handle_message:
                        success = handle_message(msg)
                    else:
                        logger.info(f"Mensaje recibido: {body}")
                        success = True
                    if success:
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                        logger.info("Mensaje procesado y eliminado de la cola.")
                        break
                except Exception as e:
                    logger.error(f"Error procesando mensaje (intento {attempt}): {e}")
                    time.sleep(2)
            if not success:
                logger.warning("No se pudo procesar el mensaje tras varios intentos.")
