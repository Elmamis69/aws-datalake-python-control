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
        
        # Obtener archivos recientes procesados
        recent_files = get_recent_processed_files(aws_conf)
        
        # Calcular archivos procesados hoy
        today_files = count_files_today(aws_conf)
        
        # Verificar si worker está corriendo
        worker_status = check_worker_status()
        
        # Obtener estadísticas por tipo de archivo
        file_stats = get_file_type_stats(aws_conf)
        
        # Obtener distribución por carpetas
        folder_stats = get_folder_distribution(aws_conf)
        
        return {
            'raw_files': raw_count,
            'raw_size': raw_size,
            'processed_files': processed_count,
            'processed_size': processed_size,
            'total_size': raw_size + processed_size,
            'queue_messages': queue_messages,
            'errors': recent_errors,
            'recent_files': recent_files,
            'today_files': today_files,
            'worker_status': worker_status,
            'file_stats': file_stats,
            'folder_stats': folder_stats,
            'aws_conf': aws_conf,  # Agregar para usar en filtros
            'timestamp': datetime.now()
        }
    except Exception as e:
        st.error(f"Error obteniendo métricas: {e}")
        return None

def get_recent_processed_files(aws_conf):
    """Obtener los últimos 5 archivos procesados"""
    try:
        import boto3
        s3 = boto3.client('s3')
        
        response = s3.list_objects_v2(
            Bucket=aws_conf['s3_processed_bucket'],
            Prefix=aws_conf['s3_processed_prefix'],
            MaxKeys=50
        )
        
        if 'Contents' not in response:
            return []
        
        # Ordenar por fecha de modificación (más recientes primero)
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        
        recent = []
        for file in files[:5]:
            recent.append({
                'nombre': file['Key'].split('/')[-1],
                'ruta': file['Key'],
                'tamaño': format_size(file['Size']),
                'fecha': file['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return recent
    except Exception as e:
        return []

def count_files_today(aws_conf):
    """Contar archivos procesados hoy"""
    try:
        import boto3
        from datetime import date
        
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(
            Bucket=aws_conf['s3_processed_bucket'],
            Prefix=aws_conf['s3_processed_prefix']
        )
        
        if 'Contents' not in response:
            return 0
        
        today = date.today()
        count = 0
        
        for obj in response['Contents']:
            if obj['LastModified'].date() == today:
                count += 1
        
        return count
    except Exception:
        return 0

def get_file_type_stats(aws_conf):
    """Obtener estadísticas por tipo de archivo"""
    try:
        import boto3
        s3 = boto3.client('s3')
        
        # Obtener TODOS los archivos del bucket RAW
        raw_response = s3.list_objects_v2(
            Bucket=aws_conf['s3_raw_bucket']
        )
        
        # Obtener archivos procesados
        processed_response = s3.list_objects_v2(
            Bucket=aws_conf['s3_processed_bucket'],
            Prefix=aws_conf['s3_processed_prefix']
        )
        
        file_types = {}
        
        # Contar tipos en RAW (incluyendo athena-results)
        for obj in raw_response.get('Contents', []):
            ext = obj['Key'].split('.')[-1].lower() if '.' in obj['Key'] else 'sin_ext'
            file_types[ext] = file_types.get(ext, 0) + 1
        
        # Contar tipos en procesados
        for obj in processed_response.get('Contents', []):
            ext = obj['Key'].split('.')[-1].lower() if '.' in obj['Key'] else 'sin_ext'
            if ext == 'parquet':
                file_types['parquet'] = file_types.get('parquet', 0) + 1
        
        return file_types
    except Exception:
        return {'csv': 0, 'json': 0, 'parquet': 0}

def get_folder_distribution(aws_conf):
    """Obtener distribución por carpetas"""
    try:
        import boto3
        s3 = boto3.client('s3')
        
        folders = {}
        
        # RAW bucket - incluir TODAS las carpetas
        raw_response = s3.list_objects_v2(
            Bucket=aws_conf['s3_raw_bucket']
        )
        
        for obj in raw_response.get('Contents', []):
            # Extraer carpeta principal
            parts = obj['Key'].split('/')
            if len(parts) > 1:
                folder = parts[0] + '/'
                folders[folder] = folders.get(folder, 0) + 1
        
        # Processed bucket
        processed_response = s3.list_objects_v2(
            Bucket=aws_conf['s3_processed_bucket'],
            Prefix=aws_conf['s3_processed_prefix']
        )
        
        for obj in processed_response.get('Contents', []):
            parts = obj['Key'].split('/')
            if len(parts) > 1:
                folder = 'processed/'
                folders[folder] = folders.get(folder, 0) + 1
        
        return folders
    except Exception:
        return {'raw/': 0, 'processed/': 0}

def get_files_advanced_filter(aws_conf, selected_date=None, file_type=None, source='processed'):
    """Obtener archivos con filtros avanzados"""
    try:
        import boto3
        from datetime import datetime
        
        s3 = boto3.client('s3')
        
        # Seleccionar bucket según el origen
        if source == 'processed':
            bucket = aws_conf['s3_processed_bucket']
            prefix = aws_conf['s3_processed_prefix']
        elif source == 'raw':
            bucket = aws_conf['s3_raw_bucket']
            prefix = aws_conf['s3_raw_prefix']
        else:  # 'all'
            bucket = aws_conf['s3_raw_bucket']
            prefix = ''
        
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        
        filtered_files = []
        
        for obj in response.get('Contents', []):
            # Filtrar por fecha si se especifica
            if selected_date and obj['LastModified'].date() != selected_date:
                continue
            
            # Filtrar por tipo de archivo si se especifica
            if file_type and file_type != 'todos':
                ext = obj['Key'].split('.')[-1].lower() if '.' in obj['Key'] else 'sin_ext'
                if ext != file_type:
                    continue
            
            filtered_files.append({
                'nombre': obj['Key'].split('/')[-1],
                'ruta': obj['Key'],
                'tamaño': format_size(obj['Size']),
                'fecha': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                'tipo': obj['Key'].split('.')[-1].lower() if '.' in obj['Key'] else 'sin_ext'
            })
        
        # Ordenar por fecha (más recientes primero)
        filtered_files.sort(key=lambda x: x['fecha'], reverse=True)
        
        return filtered_files
    except Exception:
        return []

def check_worker_status():
    """Verificar si el worker está corriendo"""
    try:
        import psutil
        import os
        from pathlib import Path
        
        current_dir = str(Path(__file__).parent.parent)
        
        # Buscar procesos de Python que contengan 'main.py worker'
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
            try:
                if proc.info['name'] and 'python' in proc.info['name'].lower():
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    
                    # Buscar tanto 'main.py worker' como el path completo
                    if ('main.py worker' in cmdline or 
                        (current_dir in cmdline and 'worker' in cmdline)):
                        
                        create_time = datetime.fromtimestamp(proc.info['create_time'])
                        uptime = datetime.now() - create_time
                        
                        return {
                            'running': True,
                            'pid': proc.info['pid'],
                            'since': f"Activo {uptime.seconds//60}m"
                        }
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        
        return {'running': False, 'pid': None, 'since': 'Detenido'}
        
    except ImportError:
        # Si psutil no está instalado
        return {'running': None, 'pid': None, 'since': 'psutil no instalado'}
    except Exception as e:
        # Cualquier otro error
        return {'running': None, 'pid': None, 'since': f'Error: {str(e)[:20]}...'}

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
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (30s)", value=False)
    
    if st.sidebar.button("🔄 Actualizar ahora"):
        st.cache_data.clear()
    
    # Obtener métricas
    metrics = get_metrics()
    
    if not metrics:
        st.error("❌ No se pudieron obtener las métricas")
        return
    
    # Métricas principales en cards (ahora con 6 columnas)
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
        queue_color = "normal" if metrics['queue_messages'] == 0 else "inverse"
        st.metric(
            label="📬 Cola SQS",
            value=metrics['queue_messages'],
            delta="En cola" if metrics['queue_messages'] > 0 else "Vacía"
        )
    
    with col6:
        error_count = len(metrics['errors'])
        error_color = "normal" if error_count == 0 else "inverse"
        st.metric(
            label="🔴 Errores (24h)",
            value=error_count,
            delta="Sin errores" if error_count == 0 else f"{error_count} errores"
        )
    
    # Estado general y worker status
    st.markdown("---")
    col1, col2 = st.columns([1, 1])
    
    with col1:
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
    
    with col2:
        # Worker Status
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
    
    # Gráficas avanzadas - FASE 2
    st.markdown("---")
    st.subheader("📊 Análisis Avanzado")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Gráfica por tipo de archivo
        st.write("**Archivos por Tipo**")
        if metrics['file_stats']:
            file_df = pd.DataFrame(list(metrics['file_stats'].items()), 
                                 columns=['Tipo', 'Cantidad'])
            
            # Colores personalizados por tipo
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
        # Gráfica circular de distribución
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
        # Eficiencia del sistema
        st.write("**Eficiencia del Sistema**")
        
        total_files = metrics['raw_files'] + metrics['processed_files']
        if total_files > 0:
            efficiency = (metrics['processed_files'] / total_files) * 100
            
            # Gauge chart simulado con métrica
            st.metric(
                label="Tasa de Procesamiento",
                value=f"{efficiency:.1f}%",
                delta="Eficiencia"
            )
            
            # Barra de progreso visual
            st.progress(efficiency / 100)
            
            # Estadísticas adicionales
            st.write(f"**Total archivos:** {total_files}")
            st.write(f"**Procesados:** {metrics['processed_files']}")
            st.write(f"**Pendientes:** {metrics['raw_files']}")
        else:
            st.info("No hay archivos para analizar")
    
    # Lista de archivos avanzada
    st.markdown("---")
    st.subheader("📄 Lista de Archivos")
    
    # Controles de filtro avanzados
    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
    
    with col1:
        # Selector de origen
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
        # Selector de tipo de archivo
        available_types = ['Todos', 'parquet', 'jsonl', 'csv', 'json', 'txt', 'metadata']
        selected_type = st.selectbox(
            "Tipo de archivo:",
            options=available_types,
            index=0
        )
    
    with col3:
        # Selector de fecha
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
        # Botón de búsqueda y estadísticas
        if st.button("🔍 Buscar archivos", type="primary"):
            st.cache_data.clear()  # Limpiar cache para nueva búsqueda
    
    # Obtener archivos con filtros
    config_path = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
    config = load_config(config_path)
    aws_conf = config['aws']
    
    filtered_files = get_files_advanced_filter(
        aws_conf, 
        selected_date, 
        selected_type.lower() if selected_type.lower() != 'todos' else None,
        selected_source
    )
    
    # Mostrar resultados con paginación
    if filtered_files:
        # Estadísticas de la búsqueda
        st.success(f"📁 **{len(filtered_files)} archivos encontrados**")
        
        # Configuración de paginación
        items_per_page = 20
        total_pages = (len(filtered_files) + items_per_page - 1) // items_per_page
        
        # Calcular archivos para la página actual
        current_page = st.session_state.get('page', 1)
        start_idx = (current_page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        display_files = filtered_files[start_idx:end_idx]
        
        # Información de paginación
        if total_pages > 1:
            st.info(f"Mostrando archivos {start_idx + 1}-{min(end_idx, len(filtered_files))} de {len(filtered_files)} (Página {current_page} de {total_pages})")
        
        # Agregar numeración a los archivos
        for i, file_info in enumerate(display_files):
            file_info['#'] = start_idx + i + 1
        
        # Crear DataFrame con numeración
        df_filtered = pd.DataFrame(display_files)
        
        # Reordenar columnas para poner # al principio
        columns_order = ['#', 'nombre', 'tipo', 'tamaño', 'fecha']
        df_filtered = df_filtered[columns_order]
        
        # Mostrar tabla
        st.dataframe(
            df_filtered,
            column_config={
                "#": st.column_config.NumberColumn(
                    "#",
                    help="Número de archivo",
                    width=60
                ),
                "nombre": st.column_config.TextColumn(
                    "Archivo",
                    help="Nombre del archivo",
                    width=None
                ),
                "tipo": st.column_config.TextColumn(
                    "Tipo",
                    help="Extensión del archivo",
                    width=80
                ),
                "tamaño": st.column_config.TextColumn(
                    "Tamaño",
                    help="Tamaño del archivo",
                    width=80
                ),
                "fecha": st.column_config.TextColumn(
                    "Fecha",
                    help="Fecha de creación/modificación",
                    width=150
                )
            },
            hide_index=True,
            width="stretch"
        )
        
        # Selector de página abajo a la derecha
        if total_pages > 1:
            col1, col2 = st.columns([3, 1])
            with col2:
                # Selector de página
                new_page = st.selectbox(
                    f"Página ({current_page} de {total_pages}):",
                    options=list(range(1, total_pages + 1)),
                    index=current_page - 1,
                    key='page_selector_bottom'
                )
                if new_page != current_page:
                    st.session_state.page = new_page
                    st.rerun()
    else:
        st.info("🔍 No se encontraron archivos con los filtros seleccionados")
    
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