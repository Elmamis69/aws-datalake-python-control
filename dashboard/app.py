"""
Dashboard de Data Lake con Streamlit
¡Rifado y en tiempo real! 🚀
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import time
import json

# Agregar el directorio padre al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.run_monitor import DataLakeMonitor, load_config

# Configuración de la página
st.set_page_config(
    page_title="🏗️ Data Lake Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado para que se vea chingón
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .status-good { color: #00ff00; }
    .status-warning { color: #ffaa00; }
    .status-error { color: #ff0000; }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=30)  # Cache por 30 segundos
def get_metrics():
    """Obtener métricas del data lake"""
    try:
        config_path = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
        config = load_config(config_path)
        monitor = DataLakeMonitor(config)
        
        aws_conf = config['aws']
        
        # Obtener datos
        raw_count, raw_size = monitor.count_s3_objects(
            aws_conf['s3_raw_bucket'], 
            aws_conf['s3_raw_prefix']
        )
        
        processed_count, processed_size = monitor.count_s3_objects(
            aws_conf['s3_processed_bucket'], 
            aws_conf['s3_processed_prefix']
        )
        
        queue_messages = monitor.get_sqs_message_count(aws_conf['sqs_queue_url'])
        recent_errors = monitor.get_recent_logs(24)
        
        return {
            'raw_files': raw_count,
            'raw_size': raw_size,
            'processed_files': processed_count,
            'processed_size': processed_size,
            'queue_messages': queue_messages,
            'errors': recent_errors,
            'timestamp': datetime.now()
        }
    except Exception as e:
        st.error(f"Error obteniendo métricas: {e}")
        return None

def format_size(size_bytes):
    """Formatear bytes a formato legible"""
    if size_bytes == 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"

def main():
    # Título principal
    st.title("🏗️ Data Lake Dashboard")
    st.markdown("### ¡Monitor en tiempo real del data lake! 🚀")
    
    # Sidebar para controles
    st.sidebar.title("⚙️ Controles")
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (30s)", value=True)
    
    if st.sidebar.button("🔄 Actualizar ahora"):
        st.cache_data.clear()
    
    # Obtener métricas
    metrics = get_metrics()
    
    if not metrics:
        st.error("❌ No se pudieron obtener las métricas")
        return
    
    # Métricas principales en cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📁 Archivos RAW",
            value=metrics['raw_files'],
            delta=format_size(metrics['raw_size'])
        )
    
    with col2:
        st.metric(
            label="✅ Procesados",
            value=metrics['processed_files'],
            delta=format_size(metrics['processed_size'])
        )
    
    with col3:
        queue_color = "normal" if metrics['queue_messages'] == 0 else "inverse"
        st.metric(
            label="📬 Cola SQS",
            value=metrics['queue_messages'],
            delta="En cola" if metrics['queue_messages'] > 0 else "Vacía"
        )
    
    with col4:
        error_count = len(metrics['errors'])
        error_color = "normal" if error_count == 0 else "inverse"
        st.metric(
            label="🔴 Errores (24h)",
            value=error_count,
            delta="Sin errores" if error_count == 0 else f"{error_count} errores"
        )
    
    # Estado general
    st.markdown("---")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Gráfica de archivos
        st.subheader("📊 Resumen de Archivos")
        
        # Crear datos para la gráfica
        data = {
            'Tipo': ['RAW', 'Procesados'],
            'Cantidad': [metrics['raw_files'], metrics['processed_files']],
            'Tamaño (MB)': [metrics['raw_size']/1024/1024, metrics['processed_size']/1024/1024]
        }
        df = pd.DataFrame(data)
        
        # Gráfica de barras
        fig = px.bar(df, x='Tipo', y='Cantidad', 
                     title="Archivos por Tipo",
                     color='Tipo',
                     color_discrete_map={'RAW': '#ff6b6b', 'Procesados': '#4ecdc4'})
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, width='stretch')
    
    with col2:
        # Estado del sistema
        st.subheader("🚦 Estado del Sistema")
        
        # Determinar estado general
        if error_count == 0 and metrics['queue_messages'] < 10:
            status = "🟢 Operativo"
            status_class = "status-good"
        elif error_count > 0 or metrics['queue_messages'] > 50:
            status = "🔴 Problemas"
            status_class = "status-error"
        else:
            status = "🟡 Atención"
            status_class = "status-warning"
        
        st.markdown(f'<h3 class="{status_class}">{status}</h3>', unsafe_allow_html=True)
        
        # Detalles del estado
        st.write(f"**Última actualización:**")
        st.write(f"{metrics['timestamp'].strftime('%H:%M:%S')}")
        
        if metrics['queue_messages'] > 0:
            st.warning(f"⚠️ {metrics['queue_messages']} mensajes en cola")
        
        if error_count > 0:
            st.error(f"🚨 {error_count} errores recientes")
    
    # Gráfica de tendencias (simulada por ahora)
    st.markdown("---")
    st.subheader("📈 Tendencias (Últimas 24h)")
    
    # Generar datos de ejemplo para tendencias
    hours = pd.date_range(start=datetime.now() - timedelta(hours=24), 
                         end=datetime.now(), freq='h')
    
    # Simular datos de procesamiento por hora
    import random
    processed_per_hour = [random.randint(0, 5) for _ in hours]
    
    trend_df = pd.DataFrame({
        'Hora': hours,
        'Archivos Procesados': processed_per_hour
    })
    
    fig_trend = px.line(trend_df, x='Hora', y='Archivos Procesados',
                       title="Archivos Procesados por Hora")
    fig_trend.update_traces(line_color='#4ecdc4', line_width=3)
    st.plotly_chart(fig_trend, width='stretch')
    
    # Errores recientes
    if metrics['errors']:
        st.markdown("---")
        st.subheader("🚨 Errores Recientes")
        
        for i, error in enumerate(metrics['errors'][-5:], 1):  # Últimos 5
            st.error(f"**{i}.** {error}")
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(30)
        st.rerun()

if __name__ == "__main__":
    main()