"""
sqs_reader.py
Lector de mensajes SQS sin procesarlos
"""
import boto3
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger("sqs_reader")

def get_sqs_messages(queue_url: str, max_messages: int = 10, session: Optional[boto3.Session] = None) -> List[Dict]:
    """
    Obtener mensajes de SQS sin eliminarlos de la cola
    
    Args:
        queue_url: URL de la cola SQS
        max_messages: Número máximo de mensajes a obtener
        session: Sesión boto3 opcional
    
    Returns:
        Lista de mensajes con metadata
    """
    sqs = (session or boto3).client('sqs')
    
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=min(max_messages, 10),  # AWS límite es 10
            WaitTimeSeconds=1,
            AttributeNames=['All'],
            MessageAttributeNames=['All']
        )
        
        messages = response.get('Messages', [])
        
        # Formatear mensajes con metadata útil
        formatted_messages = []
        for msg in messages:
            try:
                # Intentar parsear el body como JSON
                body = json.loads(msg['Body'])
            except json.JSONDecodeError:
                body = msg['Body']
            
            # Obtener timestamp del mensaje
            sent_timestamp = msg.get('Attributes', {}).get('SentTimestamp')
            if sent_timestamp:
                sent_time = datetime.fromtimestamp(int(sent_timestamp) / 1000)
            else:
                sent_time = None
            
            formatted_msg = {
                'message_id': msg['MessageId'],
                'body': body,
                'receipt_handle': msg['ReceiptHandle'],
                'sent_time': sent_time,
                'receive_count': int(msg.get('Attributes', {}).get('ApproximateReceiveCount', 0)),
                'raw_body': msg['Body']
            }
            formatted_messages.append(formatted_msg)
        
        return formatted_messages
        
    except Exception as e:
        logger.error(f"Error obteniendo mensajes SQS: {e}")
        return []

def get_queue_attributes(queue_url: str, session: Optional[boto3.Session] = None) -> Dict:
    """
    Obtener atributos de la cola SQS
    
    Args:
        queue_url: URL de la cola SQS
        session: Sesión boto3 opcional
    
    Returns:
        Diccionario con atributos de la cola
    """
    sqs = (session or boto3).client('sqs')
    
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['All']
        )
        
        attributes = response.get('Attributes', {})
        
        return {
            'messages_available': int(attributes.get('ApproximateNumberOfMessages', 0)),
            'messages_in_flight': int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0)),
            'messages_delayed': int(attributes.get('ApproximateNumberOfMessagesDelayed', 0)),
            'created_timestamp': attributes.get('CreatedTimestamp'),
            'last_modified_timestamp': attributes.get('LastModifiedTimestamp'),
            'visibility_timeout': int(attributes.get('VisibilityTimeout', 0)),
            'message_retention_period': int(attributes.get('MessageRetentionPeriod', 0)),
            'max_receive_count': attributes.get('RedrivePolicy', {}).get('maxReceiveCount', 'N/A')
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo atributos de cola: {e}")
        return {}

def display_messages_terminal(queue_url: str, max_messages: int = 10, details: bool = False):
    """
    Mostrar mensajes SQS en terminal
    
    Args:
        queue_url: URL de la cola SQS
        max_messages: Número máximo de mensajes a mostrar
        details: Si mostrar detalles completos
    """
    print("📬 Consultando mensajes SQS...")
    
    # Obtener atributos de la cola
    queue_attrs = get_queue_attributes(queue_url)
    total_messages = queue_attrs.get('messages_available', 0)
    messages_in_flight = queue_attrs.get('messages_in_flight', 0)
    messages_delayed = queue_attrs.get('messages_delayed', 0)
    
    print(f"\n📊 Estado de la Cola:")
    print(f"   • Mensajes disponibles: {total_messages}")
    print(f"   • Mensajes en procesamiento: {messages_in_flight}")
    print(f"   • Mensajes retrasados: {messages_delayed}")
    
    # Explicar el estado
    if messages_in_flight > 0:
        print(f"\n💡 Información sobre mensajes en procesamiento:")
        print(f"   Los {messages_in_flight} mensajes están siendo procesados por otro consumidor")
        print(f"   o están en período de visibility timeout (no visibles temporalmente)")
        print(f"   Timeout de visibilidad: {queue_attrs.get('visibility_timeout', 0)} segundos")
    
    if total_messages == 0 and messages_in_flight == 0 and messages_delayed == 0:
        print("\n✅ No hay mensajes en cola")
        return
    elif total_messages == 0:
        print("\n⏳ No hay mensajes disponibles para leer en este momento")
        if messages_in_flight > 0:
            print("   (pero hay mensajes siendo procesados)")
        return
    
    # Obtener mensajes
    messages = get_sqs_messages(queue_url, max_messages)
    
    if not messages:
        print("\n✅ No se pudieron obtener mensajes (pueden estar siendo procesados)")
        return
    
    print(f"\n📋 Mensajes en Cola ({len(messages)} de {total_messages}):")
    print("=" * 60)
    
    for i, msg in enumerate(messages, 1):
        print(f"\n📨 Mensaje {i}:")
        print(f"   ID: {msg['message_id']}")
        
        if msg['sent_time']:
            print(f"   Enviado: {msg['sent_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"   Recibido {msg['receive_count']} veces")
        
        if details:
            print(f"   Contenido completo:")
            if isinstance(msg['body'], dict):
                print(f"      {json.dumps(msg['body'], indent=6, ensure_ascii=False)}")
            else:
                print(f"      {msg['body']}")
        else:
            # Mostrar resumen del contenido
            if isinstance(msg['body'], dict):
                # Si es JSON, mostrar keys principales
                keys = list(msg['body'].keys())[:3]
                print(f"   Contenido: JSON con keys: {keys}")
            else:
                # Si es texto, mostrar primeros 100 caracteres
                content = str(msg['body'])[:100]
                if len(str(msg['body'])) > 100:
                    content += "..."
                print(f"   Contenido: {content}")
    
    if len(messages) < total_messages:
        print(f"\n💡 Hay {total_messages - len(messages)} mensajes más en cola")
        print("   Use --details para ver contenido completo")
        print("   Use --max-messages N para ver más mensajes")