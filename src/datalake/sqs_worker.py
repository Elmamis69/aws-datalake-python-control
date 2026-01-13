"""
SQS Worker - Procesador Automático de Archivos

Este módulo implementa un worker que:
1. Escucha continuamente mensajes de una cola SQS
2. Procesa archivos cuando recibe notificaciones
3. Maneja reintentos automáticos en caso de errores
4. Elimina mensajes procesados exitosamente de la cola

El worker es el corazón del procesamiento automático del Data Lake.
Cuando se sube un archivo a S3 RAW, se envía un mensaje a SQS,
y este worker lo procesa automáticamente.

Uso típico:
    from sqs_worker import run_sqs_worker
    
    def procesar_archivo(mensaje):
        # Lógica de procesamiento
        return True  # True si exitoso
    
    run_sqs_worker(
        queue_url="https://sqs.region.amazonaws.com/account/queue",
        handle_message=procesar_archivo
    )
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
    Worker principal para consumir mensajes de SQS y procesar eventos de archivos
    
    Este worker implementa un patrón de polling continuo:
    - Consulta la cola SQS cada poll_interval segundos
    - Procesa mensajes usando la función handle_message
    - Reintenta automáticamente en caso de errores
    - Se detiene después de max_empty_polls consultas vacías (opcional)
    
    Args:
        queue_url (str): URL completa de la cola SQS
        session (boto3.Session, optional): Sesión AWS personalizada
        handle_message (callable, optional): Función que procesa cada mensaje.
                                           Debe retornar True si fue exitoso.
        poll_interval (int): Segundos entre consultas a la cola
        max_retries (int): Número de reintentos por mensaje fallido
        max_empty_polls (int): Máximo de consultas vacías antes de terminar.
                              None = ejecutar indefinidamente
    
    Returns:
        None: El worker ejecuta hasta ser interrumpido o alcanzar max_empty_polls
    """
    sqs = (session or boto3).client('sqs')
    logger.info(f"🚀 Iniciando worker SQS en {queue_url}")
    logger.info(f"⚙️  Configuración: poll_interval={poll_interval}s, max_retries={max_retries}")
    
    empty_polls = 0
    
    while True:
        # Consultar cola SQS con long polling (WaitTimeSeconds=20)
        # Esto reduce costos y mejora la eficiencia
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,  # Procesar de a uno para mejor control
            WaitTimeSeconds=20      # Long polling - espera hasta 20s por mensajes
        )
        messages = resp.get('Messages', [])
        
        if not messages:
            empty_polls += 1
            logger.info(f"📭 No hay mensajes ({empty_polls}/{max_empty_polls or '∞'})")
            
            # Terminar si se alcanza el límite de consultas vacías
            if max_empty_polls and empty_polls >= max_empty_polls:
                logger.info("🛑 Máximo de polls vacíos alcanzado. Terminando worker.")
                break
                
            time.sleep(poll_interval)
            continue
        
        # Resetear contador si hay mensajes
        empty_polls = 0
        
        # Procesar cada mensaje
        for msg in messages:
            receipt = msg['ReceiptHandle']  # Necesario para eliminar el mensaje
            body = msg['Body']              # Contenido del mensaje (ubicación del archivo)
            success = False
            
            # Reintentos automáticos en caso de error
            for attempt in range(1, max_retries+1):
                try:
                    logger.info(f"📨 Procesando mensaje (intento {attempt}/{max_retries}): {body}")
                    
                    if handle_message:
                        success = handle_message(msg)
                    else:
                        # Comportamiento por defecto: solo loggear
                        logger.info(f"📄 Mensaje recibido: {body}")
                        success = True
                        
                    if success:
                        # Eliminar mensaje de la cola solo si se procesó exitosamente
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                        logger.info("✅ Mensaje procesado y eliminado de la cola.")
                        break
                        
                except Exception as e:
                    logger.error(f"❌ Error procesando mensaje (intento {attempt}): {e}")
                    if attempt < max_retries:
                        time.sleep(2)  # Esperar antes del siguiente intento
                    
            if not success:
                logger.warning(f"⚠️  No se pudo procesar el mensaje tras {max_retries} intentos.")
                logger.warning("   El mensaje permanecerá en la cola y será reintentado más tarde.")
