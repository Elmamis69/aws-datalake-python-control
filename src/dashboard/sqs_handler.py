"""
Módulo para manejo de SQS en el dashboard
"""

from src.datalake.sqs_reader import get_sqs_messages, get_queue_attributes

def get_sqs_messages_for_dashboard(queue_url, max_messages=20):
    """Obtener mensajes SQS para mostrar en dashboard"""
    try:
        # Usar la función mejorada que ya hace múltiples consultas
        messages = get_sqs_messages(queue_url, max_messages)
        queue_attrs = get_queue_attributes(queue_url)
        
        return {
            'messages': messages,
            'queue_attributes': queue_attrs,
            'total_messages': queue_attrs.get('messages_available', 0),
            'messages_in_flight': queue_attrs.get('messages_in_flight', 0),
            'messages_delayed': queue_attrs.get('messages_delayed', 0)
        }
    except Exception as e:
        return {
            'messages': [],
            'queue_attributes': {},
            'total_messages': 0,
            'messages_in_flight': 0,
            'messages_delayed': 0,
            'error': str(e)
        }