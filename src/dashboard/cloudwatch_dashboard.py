"""
CloudWatch Dashboard - Visualización de métricas en Streamlit

Componentes para mostrar:
- Métricas en tiempo real
- Gráficos de tendencias
- Estado de alarmas
- Alertas activas
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
import boto3
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

def render_cloudwatch_tab():
    """Renderizar tab de CloudWatch en Streamlit"""
    st.header("📊 Monitoreo CloudWatch")
    
    # Importar aquí para evitar errores de importación circular
    try:
        from src.cloudwatch_monitor import CloudWatchMonitor, DataLakeMetrics
        monitor = CloudWatchMonitor()
    except Exception as e:
        st.error(f"❌ Error conectando con CloudWatch: {e}")
        return
    
    # Configuración de tiempo
    col1, col2 = st.columns(2)
    with col1:
        hours = st.selectbox("Período de tiempo", [1, 6, 12, 24, 48], index=3)
    with col2:
        auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
    
    if auto_refresh:
        st.rerun()
    
    # Métricas principales
    render_key_metrics(monitor, hours)
    
    # Gráficos de tendencias
    render_trend_charts(monitor, hours)
    
    # Estado de alarmas
    render_alarms_status(monitor)
    
    # Métricas detalladas
    render_detailed_metrics(monitor, hours)

def render_key_metrics(monitor: CloudWatchMonitor, hours: int):
    """Mostrar métricas clave en cards"""
    st.subheader("🎯 Métricas Principales")
    
    # Obtener métricas
    files_processed = monitor.get_metrics('FilesProcessed', hours)
    files_success = monitor.get_metrics('FilesSuccess', hours)
    files_error = monitor.get_metrics('FilesError', hours)
    processing_time = monitor.get_metrics('ProcessingTime', hours)
    
    # Calcular totales
    total_files = sum([point['Sum'] for point in files_processed])
    total_success = sum([point['Sum'] for point in files_success])
    total_errors = sum([point['Sum'] for point in files_error])
    avg_time = sum([point['Average'] for point in processing_time]) / len(processing_time) if processing_time else 0
    
    # Mostrar en columnas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📁 Archivos Procesados",
            value=int(total_files),
            delta=f"Últimas {hours}h"
        )
    
    with col2:
        success_rate = (total_success / total_files * 100) if total_files > 0 else 0
        st.metric(
            label="✅ Tasa de Éxito",
            value=f"{success_rate:.1f}%",
            delta="↗️" if success_rate > 90 else "⚠️"
        )
    
    with col3:
        st.metric(
            label="❌ Errores",
            value=int(total_errors),
            delta="🔴" if total_errors > 0 else "🟢"
        )
    
    with col4:
        st.metric(
            label="⏱️ Tiempo Promedio",
            value=f"{avg_time:.1f}s",
            delta="Procesamiento"
        )

def render_trend_charts(monitor: CloudWatchMonitor, hours: int):
    """Mostrar gráficos de tendencias"""
    st.subheader("📈 Tendencias")
    
    # Obtener datos
    files_data = monitor.get_metrics('FilesProcessed', hours)
    error_data = monitor.get_metrics('FilesError', hours)
    time_data = monitor.get_metrics('ProcessingTime', hours)
    
    if not files_data:
        st.info("📊 No hay datos de métricas disponibles")
        return
    
    # Crear DataFrames
    df_files = pd.DataFrame(files_data)
    df_errors = pd.DataFrame(error_data) if error_data else pd.DataFrame()
    df_time = pd.DataFrame(time_data) if time_data else pd.DataFrame()
    
    # Gráfico de archivos procesados
    col1, col2 = st.columns(2)
    
    with col1:
        if not df_files.empty:
            fig_files = px.line(
                df_files, 
                x='Timestamp', 
                y='Sum',
                title='Archivos Procesados por Tiempo',
                labels={'Sum': 'Archivos', 'Timestamp': 'Tiempo'}
            )
            fig_files.update_layout(height=300)
            st.plotly_chart(fig_files, use_container_width=True)
    
    with col2:
        if not df_errors.empty:
            fig_errors = px.bar(
                df_errors, 
                x='Timestamp', 
                y='Sum',
                title='Errores por Tiempo',
                labels={'Sum': 'Errores', 'Timestamp': 'Tiempo'},
                color_discrete_sequence=['red']
            )
            fig_errors.update_layout(height=300)
            st.plotly_chart(fig_errors, use_container_width=True)
        else:
            st.success("🎉 Sin errores en el período seleccionado")
    
    # Gráfico de tiempo de procesamiento
    if not df_time.empty:
        fig_time = px.line(
            df_time, 
            x='Timestamp', 
            y='Average',
            title='Tiempo Promedio de Procesamiento',
            labels={'Average': 'Segundos', 'Timestamp': 'Tiempo'}
        )
        fig_time.update_layout(height=300)
        st.plotly_chart(fig_time, use_container_width=True)

def render_alarms_status(monitor: CloudWatchMonitor):
    """Mostrar estado de alarmas"""
    st.subheader("🚨 Estado de Alarmas")
    
    try:
        alarms = monitor.get_alarms()
        
        if not alarms:
            st.info("📋 No hay alarmas configuradas")
            return
        
        # Filtrar alarmas del Data Lake
        datalake_alarms = [alarm for alarm in alarms if 'DataLake' in alarm['AlarmName']]
        
        if not datalake_alarms:
            st.info("📋 No hay alarmas del Data Lake configuradas")
            return
        
        # Mostrar alarmas en tabla
        alarm_data = []
        for alarm in datalake_alarms:
            state = alarm['StateValue']
            state_emoji = {
                'OK': '🟢',
                'ALARM': '🔴',
                'INSUFFICIENT_DATA': '🟡'
            }.get(state, '⚪')
            
            alarm_data.append({
                'Estado': f"{state_emoji} {state}",
                'Nombre': alarm['AlarmName'],
                'Métrica': alarm['MetricName'],
                'Umbral': alarm['Threshold'],
                'Última Actualización': alarm['StateUpdatedTimestamp'].strftime('%H:%M:%S')
            })
        
        df_alarms = pd.DataFrame(alarm_data)
        st.dataframe(df_alarms, use_container_width=True)
        
        # Resumen de estados
        states = [alarm['StateValue'] for alarm in datalake_alarms]
        ok_count = states.count('OK')
        alarm_count = states.count('ALARM')
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("🟢 OK", ok_count)
        with col2:
            st.metric("🔴 ALARM", alarm_count)
        with col3:
            st.metric("🟡 SIN DATOS", states.count('INSUFFICIENT_DATA'))
            
    except Exception as e:
        st.error(f"❌ Error obteniendo alarmas: {e}")

def render_detailed_metrics(monitor: CloudWatchMonitor, hours: int):
    """Mostrar métricas detalladas"""
    st.subheader("📋 Métricas Detalladas")
    
    # Selector de métrica
    metric_options = [
        'FilesProcessed', 'FilesSuccess', 'FilesError',
        'ProcessingTime', 'SQSMessagesReceived', 'SQSMessagesProcessed',
        'S3Operations', 'GlueTablesUpdated'
    ]
    
    selected_metric = st.selectbox("Seleccionar métrica", metric_options)
    
    # Obtener y mostrar datos
    metric_data = monitor.get_metrics(selected_metric, hours)
    
    if metric_data:
        df = pd.DataFrame(metric_data)
        
        # Estadísticas
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total", f"{df['Sum'].sum():.0f}")
        with col2:
            st.metric("Promedio", f"{df['Average'].mean():.2f}")
        with col3:
            st.metric("Máximo", f"{df['Maximum'].max():.0f}")
        
        # Tabla de datos
        st.dataframe(df[['Timestamp', 'Sum', 'Average', 'Maximum']], use_container_width=True)
    else:
        st.info(f"📊 No hay datos para la métrica {selected_metric}")

def render_cloudwatch_metrics():
    """Función principal para renderizar métricas de CloudWatch"""
    render_cloudwatch_tab()

def setup_cloudwatch_alarms():
    """Configurar alarmas de CloudWatch desde Streamlit"""
    st.subheader("⚙️ Configurar Alarmas")
    
    with st.expander("Crear Nueva Alarma"):
        col1, col2 = st.columns(2)
        
        with col1:
            alarm_name = st.text_input("Nombre de la alarma")
            metric_name = st.selectbox("Métrica", [
                'FilesError', 'ProcessingTime', 'SQSSuccessRate'
            ])
            threshold = st.number_input("Umbral", min_value=0.0, value=5.0)
        
        with col2:
            comparison = st.selectbox("Comparación", [
                'GreaterThanThreshold', 'LessThanThreshold'
            ])
            sns_topic = st.text_input("SNS Topic ARN (opcional)")
        
        if st.button("Crear Alarma"):
            try:
                from src.cloudwatch_monitor import CloudWatchMonitor
                monitor = CloudWatchMonitor()
                monitor.create_alarm(
                    alarm_name=alarm_name,
                    metric_name=metric_name,
                    threshold=threshold,
                    comparison=comparison,
                    sns_topic=sns_topic if sns_topic else None
                )
                st.success(f"✅ Alarma '{alarm_name}' creada exitosamente")
            except Exception as e:
                st.error(f"❌ Error creando alarma: {e}")