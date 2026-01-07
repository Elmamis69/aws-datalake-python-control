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
    max_retries: int = 3,
    max_empty_polls: int = None
):
    """
    Worker principal para consumir mensajes de SQS y procesar eventos.
    Args:
        queue_url (str): URL de la cola SQS.
        session (boto3.Session, optional): Sesión boto3.
        handle_message (callable, optional): Función que procesa el mensaje. Debe retornar True si fue exitoso.
        poll_interval (int): Segundos entre polls.
        max_retries (int): Reintentos por mensaje fallido.
        max_empty_polls (int): Número máximo de polls vacíos antes de terminar. None = infinito.
    """
    sqs = (session or boto3).client('sqs')
    logger.info(f"Iniciando worker SQS en {queue_url}")
    
    empty_polls = 0
    
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )
        messages = resp.get('Messages', [])
        
        if not messages:
            empty_polls += 1
            logger.info(f"No hay mensajes ({empty_polls}/{max_empty_polls or '∞'})")
            
            if max_empty_polls and empty_polls >= max_empty_polls:
                logger.info("Máximo de polls vacíos alcanzado. Terminando worker.")
                break
                
            time.sleep(poll_interval)
            continue
        
        # Resetear contador si hay mensajes
        empty_polls = 0
        
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
