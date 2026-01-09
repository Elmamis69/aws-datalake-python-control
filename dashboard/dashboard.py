"""
Dashboard Principal - Módulo de análisis de datos
Proporciona funciones para análisis y visualización de datos
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import boto3
from typing import Dict, List, Any

class DataAnalyzer:
    """Analizador de datos para el dashboard"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def get_s3_metrics(self, bucket: str, prefix: str = "") -> Dict[str, Any]:
        """Obtener métricas de S3"""
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            objects = response.get('Contents', [])
            
            total_files = len(objects)
            total_size = sum(obj['Size'] for obj in objects)
            
            # Archivos por día (últimos 7 días)
            today = datetime.now().date()
            daily_counts = {}
            
            for i in range(7):
                date = today - timedelta(days=i)
                daily_counts[date.strftime('%Y-%m-%d')] = 0
            
            for obj in objects:
                obj_date = obj['LastModified'].date().strftime('%Y-%m-%d')
                if obj_date in daily_counts:
                    daily_counts[obj_date] += 1
            
            return {
                'total_files': total_files,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'daily_counts': daily_counts
            }
        except Exception as e:
            return {'error': str(e)}
    
    def create_files_chart(self, daily_counts: Dict[str, int]):
        """Crear gráfico de archivos por día"""
        dates = list(daily_counts.keys())
        counts = list(daily_counts.values())
        
        fig = px.bar(
            x=dates, 
            y=counts,
            title="Archivos procesados por día (últimos 7 días)",
            labels={'x': 'Fecha', 'y': 'Número de archivos'}
        )
        
        fig.update_layout(
            xaxis_tickangle=-45,
            height=400
        )
        
        return fig
    
    def create_size_chart(self, total_size_mb: float):
        """Crear gráfico de tamaño de datos"""
        fig = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = total_size_mb,
            title = {'text': "Tamaño total de datos (MB)"},
            gauge = {
                'axis': {'range': [None, max(100, total_size_mb * 1.2)]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, total_size_mb * 0.5], 'color': "lightgray"},
                    {'range': [total_size_mb * 0.5, total_size_mb], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': total_size_mb * 0.9
                }
            }
        ))
        
        fig.update_layout(height=400)
        return fig

def get_sample_data() -> pd.DataFrame:
    """Generar datos de ejemplo para el dashboard"""
    dates = pd.date_range(start='2026-01-01', end='2026-01-08', freq='D')
    data = {
        'fecha': dates,
        'archivos_procesados': [15, 23, 18, 31, 27, 19, 25],
        'errores': [1, 0, 2, 1, 0, 1, 0],
        'tiempo_procesamiento': [45, 67, 52, 89, 73, 48, 61]
    }
    return pd.DataFrame(data)

def create_processing_summary():
    """Crear resumen de procesamiento"""
    df = get_sample_data()
    
    summary = {
        'total_archivos': df['archivos_procesados'].sum(),
        'total_errores': df['errores'].sum(),
        'tiempo_promedio': df['tiempo_procesamiento'].mean(),
        'ultimo_procesamiento': df['fecha'].max().strftime('%Y-%m-%d')
    }
    
    return summary