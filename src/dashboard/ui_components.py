"""
Módulo para componentes de UI del dashboard
"""

import streamlit as st
import plotly.express as px
import pandas as pd

def render_metrics_cards(metrics):
    """Renderizar las tarjetas de métricas principales"""
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
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
        st.metric(
            label="📊 Total Datos",
            value=format_size(metrics['total_size']),
            delta="Acumulado"
        )
    
    with col4:
        st.metric(
            label="🔥 Hoy",
            value=metrics['today_files'],
            delta="Procesados"
        )
    
    with col5:
        st.metric(
            label="📬 Cola SQS",
            value=metrics['queue_messages'],
            delta="En cola" if metrics['queue_messages'] > 0 else "Vacía"
        )
    
    with col6:
        error_count = len(metrics['errors'])
        st.metric(
            label="🔴 Errores (24h)",
            value=error_count,
            delta="Sin errores" if error_count == 0 else f"{error_count} errores"
        )

def render_system_status(metrics):
    """Renderizar estado del sistema y worker"""
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("🚦 Estado del Sistema")
        
        error_count = len(metrics['errors'])
        
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
        
        st.write(f"**Última actualización:**")
        st.write(f"{metrics['timestamp'].strftime('%H:%M:%S')}")
        
        if metrics['queue_messages'] > 0:
            st.warning(f"⚠️ {metrics['queue_messages']} mensajes en cola")
        
        if error_count > 0:
            st.error(f"🚨 {error_count} errores recientes")
    
    with col2:
        st.subheader("🤖 Worker Status")
        
        worker = metrics['worker_status']
        if worker['running'] is True:
            st.success("🟢 Worker Activo")
            st.write(f"**PID:** {worker['pid']}")
            st.write(f"**Tiempo:** {worker['since']}")
        elif worker['running'] is False:
            st.error("🔴 Worker Detenido")
            st.write("**Estado:** No está corriendo")
            st.write("⚠️ Ejecuta:")
            st.code("python main.py worker")
        else:
            st.warning("🟡 Estado Desconocido")
            st.write(f"**Info:** {worker['since']}")
            if 'psutil' in worker['since']:
                st.write("💡 Instala: `pip install psutil`")

def render_advanced_charts(metrics):
    """Renderizar gráficas avanzadas"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**Archivos por Tipo**")
        if metrics['file_stats']:
            file_df = pd.DataFrame(list(metrics['file_stats'].items()), 
                                 columns=['Tipo', 'Cantidad'])
            
            colors = {
                'csv': '#ff6b6b',
                'json': '#4ecdc4', 
                'jsonl': '#45b7d1',
                'parquet': '#96ceb4',
                'txt': '#feca57',
                'sin_ext': '#ff9ff3'
            }
            
            fig_types = px.bar(file_df, x='Tipo', y='Cantidad',
                             color='Tipo',
                             color_discrete_map=colors)
            fig_types.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig_types, use_container_width=True)
        else:
            st.info("No hay datos de tipos de archivo")
    
    with col2:
        st.write("**Distribución por Carpetas**")
        if metrics['folder_stats']:
            folder_df = pd.DataFrame(list(metrics['folder_stats'].items()), 
                                   columns=['Carpeta', 'Archivos'])
            
            fig_folders = px.pie(folder_df, values='Archivos', names='Carpeta',
                               color_discrete_sequence=px.colors.qualitative.Set3)
            fig_folders.update_layout(height=300)
            st.plotly_chart(fig_folders, use_container_width=True)
        else:
            st.info("No hay datos de carpetas")
    
    with col3:
        st.write("**Eficiencia del Sistema**")
        
        total_files = metrics['raw_files'] + metrics['processed_files']
        if total_files > 0:
            efficiency = (metrics['processed_files'] / total_files) * 100
            
            st.metric(
                label="Tasa de Procesamiento",
                value=f"{efficiency:.1f}%",
                delta="Eficiencia"
            )
            
            st.progress(efficiency / 100)
            
            st.write(f"**Total archivos:** {total_files}")
            st.write(f"**Procesados:** {metrics['processed_files']}")
            st.write(f"**Pendientes:** {metrics['raw_files']}")
        else:
            st.info("No hay archivos para analizar")

def render_file_filters():
    """Renderizar controles de filtro de archivos"""
    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
    
    with col1:
        source_options = {
            'processed': 'Procesados',
            'raw': 'RAW',
            'all': 'Todos los buckets'
        }
        selected_source = st.selectbox(
            "Origen:",
            options=list(source_options.keys()),
            format_func=lambda x: source_options[x],
            index=0
        )
    
    with col2:
        available_types = ['Todos', 'parquet', 'jsonl', 'csv', 'json', 'txt', 'metadata']
        selected_type = st.selectbox(
            "Tipo de archivo:",
            options=available_types,
            index=0
        )
    
    with col3:
        from datetime import date, timedelta
        
        use_date_filter = st.checkbox("Filtrar por fecha")
        if use_date_filter:
            selected_date = st.date_input(
                "Fecha:",
                value=date.today(),
                max_value=date.today(),
                min_value=date.today() - timedelta(days=30)
            )
        else:
            selected_date = None
    
    with col4:
        if st.button("🔍 Buscar archivos", type="primary"):
            st.cache_data.clear()
    
    return selected_source, selected_type, selected_date

def format_size(size_bytes):
    """Formatear bytes a formato legible"""
    if size_bytes == 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"