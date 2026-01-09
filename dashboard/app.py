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
                'bucket': bucket,
                'tamaño': format_size(obj['Size']),
                'fecha': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                'tipo': obj['Key'].split('.')[-1].lower() if '.' in obj['Key'] else 'sin_ext'
            })
        
        # Ordenar por fecha (más recientes primero)
        filtered_files.sort(key=lambda x: x['fecha'], reverse=True)
        
        return filtered_files
    except Exception:
        return []

def read_file_from_s3(bucket, key, file_type):
    """Leer archivo desde S3 y retornar DataFrame"""
    try:
        import boto3
        import pandas as pd
        
        s3_path = f"s3://{bucket}/{key}"
        
        if file_type == 'parquet':
            df = pd.read_parquet(s3_path)
        elif file_type in ['json', 'jsonl']:
            df = pd.read_json(s3_path, lines=True if file_type == 'jsonl' else False)
        elif file_type == 'csv':
            df = pd.read_csv(s3_path)
        else:
            return None, f"Tipo de archivo no soportado: {file_type}"
        
        return df, None
    except Exception as e:
        return None, f"Error leyendo archivo: {str(e)}"

def download_file_from_s3(bucket, key):
    """Descargar archivo desde S3 y retornar contenido"""
    try:
        import boto3
        import io
        
        s3 = boto3.client('s3')
        
        # Crear buffer en memoria
        buffer = io.BytesIO()
        s3.download_fileobj(bucket, key, buffer)
        buffer.seek(0)
        
        return buffer.getvalue(), None
    except Exception as e:
        return None, f"Error descargando archivo: {str(e)}"

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
        
        # Crear DataFrame con numeración (sin mostrar bucket y ruta)
        df_filtered = pd.DataFrame(display_files)
        
        # Reordenar columnas para poner # al principio (sin bucket y ruta para la vista)
        columns_order = ['#', 'nombre', 'tipo', 'tamaño', 'fecha']
        df_display = df_filtered[columns_order]
        
        # Agregar indicador de archivos legibles
        df_display['📖'] = ['✅' if f['tipo'] in ['parquet', 'json', 'jsonl', 'csv'] else '❌' for f in display_files]
        
        # Mostrar tabla
        st.dataframe(
            df_display,
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
                ),
                "📖": st.column_config.TextColumn(
                    "Legible",
                    help="✅ = Se puede leer y analizar, ❌ = Solo descarga",
                    width=70
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
    
    # Lector de archivos
    st.markdown("---")
    st.subheader("📖 Lector de Archivos")
    
    # Selector de archivo para leer - TODOS los archivos
    if filtered_files:
        col1, col2, col3 = st.columns([3, 1, 1])
        
        with col1:
            # Crear opciones para el selectbox - TODOS los archivos (sin límite)
            file_options = {}
            for i, file_info in enumerate(filtered_files):  # Sin límite de 50
                display_name = f"{i+1:2d}. {file_info['nombre']} ({file_info['tipo'].upper()}, {file_info['tamaño']})"
                file_options[display_name] = file_info
            
            selected_file_display = st.selectbox(
                f"Selecciona un archivo ({len(filtered_files)} disponibles):",
                options=list(file_options.keys()),
                index=0 if file_options else None
            )
        
        with col2:
            # Botón para leer (solo archivos compatibles)
            if selected_file_display:
                selected_file = file_options[selected_file_display]
                is_readable = selected_file['tipo'] in ['parquet', 'json', 'jsonl', 'csv']
                
                read_button = st.button(
                    "📖 Leer Archivo", 
                    type="primary",
                    disabled=not is_readable,
                    help="Solo archivos parquet, json, jsonl, csv" if not is_readable else "Leer y analizar archivo"
                )
        
        with col3:
            # Botón para descargar (todos los archivos)
            if selected_file_display:
                download_button = st.button(
                    "⬇️ Descargar", 
                    type="secondary",
                    help="Descargar archivo completo"
                )
        
        # Mostrar información del archivo seleccionado
        if selected_file_display:
            selected_file = file_options[selected_file_display]
            
            # Info del archivo
            st.info(f"📁 **Archivo:** {selected_file['nombre']} | **Tipo:** {selected_file['tipo'].upper()} | **Tamaño:** {selected_file['tamaño']} | **Fecha:** {selected_file['fecha']}")
            
            # Descargar archivo
            if download_button:
                with st.spinner(f"Descargando {selected_file['nombre']}..."):
                    file_content, error = download_file_from_s3(
                        selected_file['bucket'],
                        selected_file['ruta']
                    )
                
                if error:
                    st.error(f"❌ {error}")
                else:
                    st.success(f"✅ Archivo descargado: **{selected_file['nombre']}**")
                    
                    # Botón de descarga
                    st.download_button(
                        label="💾 Guardar archivo",
                        data=file_content,
                        file_name=selected_file['nombre'],
                        mime="application/octet-stream"
                    )
            
            # Leer archivo (solo si es compatible)
            if read_button and is_readable:
                with st.spinner(f"Leyendo {selected_file['nombre']}..."):
                    df, error = read_file_from_s3(
                        selected_file['bucket'],
                        selected_file['ruta'],
                        selected_file['tipo']
                    )
                
                if error:
                    st.error(f"❌ {error}")
                else:
                    st.success(f"✅ Archivo leído exitosamente: **{selected_file['nombre']}**")
                    
                    # Información del DataFrame
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("📊 Filas", f"{df.shape[0]:,}")
                    with col2:
                        st.metric("📋 Columnas", df.shape[1])
                    with col3:
                        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                        st.metric("💾 Memoria", f"{memory_mb:.1f} MB")
                    with col4:
                        # Botón para abrir en nueva pestaña (exportar como CSV)
                        csv_data = df.to_csv(index=False)
                        st.download_button(
                            "🔗 Exportar CSV",
                            data=csv_data,
                            file_name=f"{selected_file['nombre'].split('.')[0]}_export.csv",
                            mime="text/csv",
                            help="Descargar como CSV para abrir en Excel/Google Sheets"
                        )
                    
                    # Tabs para diferentes vistas
                    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🔍 Vista Previa", "📊 Estadísticas", "🏷️ Tipos de Datos", "📈 Gráficas", "🔍 Explorar"])
                    
                    with tab1:
                        # Controles de vista previa
                        col1, col2 = st.columns([1, 1])
                        with col1:
                            show_rows = st.slider("Filas a mostrar:", 5, min(100, len(df)), 10)
                        with col2:
                            view_mode = st.radio("Vista:", ["Primeras filas", "Últimas filas", "Muestra aleatoria"], horizontal=True)
                        
                        if view_mode == "Primeras filas":
                            st.write(f"**Primeras {show_rows} filas:**")
                            st.dataframe(df.head(show_rows), use_container_width=True)
                        elif view_mode == "Últimas filas":
                            st.write(f"**Últimas {show_rows} filas:**")
                            st.dataframe(df.tail(show_rows), use_container_width=True)
                        else:
                            st.write(f"**Muestra aleatoria de {show_rows} filas:**")
                            st.dataframe(df.sample(min(show_rows, len(df))), use_container_width=True)
                    
                    with tab2:
                        # Estadísticas básicas
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            st.write("**Estadísticas de columnas numéricas:**")
                            st.dataframe(df[numeric_cols].describe(), use_container_width=True)
                        
                        # Información general
                        st.write("**Resumen por columna:**")
                        info_data = {
                            'Columna': df.columns,
                            'Tipo': [str(dtype) for dtype in df.dtypes],
                            'No Nulos': [f"{df[col].count():,}" for col in df.columns],
                            'Nulos': [f"{df[col].isnull().sum():,}" for col in df.columns],
                            '% Nulos': [f"{(df[col].isnull().sum() / len(df) * 100):.1f}%" for col in df.columns],
                            'Únicos': [f"{df[col].nunique():,}" for col in df.columns]
                        }
                        info_df = pd.DataFrame(info_data)
                        st.dataframe(info_df, use_container_width=True)
                    
                    with tab3:
                        st.write("**Análisis detallado por columna:**")
                        
                        # Selector de columna
                        selected_col = st.selectbox("Selecciona una columna:", df.columns)
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write(f"**Información de '{selected_col}':**")
                            col_info = {
                                'Tipo de dato': str(df[selected_col].dtype),
                                'Valores totales': f"{len(df):,}",
                                'Valores únicos': f"{df[selected_col].nunique():,}",
                                'Valores nulos': f"{df[selected_col].isnull().sum():,}",
                                'Porcentaje nulos': f"{(df[selected_col].isnull().sum() / len(df) * 100):.2f}%"
                            }
                            
                            for key, value in col_info.items():
                                st.write(f"- **{key}:** {value}")
                        
                        with col2:
                            st.write(f"**Valores más frecuentes en '{selected_col}':**")
                            if df[selected_col].nunique() <= 50:
                                value_counts = df[selected_col].value_counts().head(10)
                                st.dataframe(value_counts.to_frame('Frecuencia'), use_container_width=True)
                            else:
                                st.info("Demasiados valores únicos para mostrar frecuencias")
                    
                    with tab4:
                        # Gráficas automáticas mejoradas
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        
                        if len(numeric_cols) > 0:
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                # Selector de columna numérica
                                selected_numeric = st.selectbox("Columna numérica:", numeric_cols)
                                
                                # Histograma
                                st.write(f"**Distribución de {selected_numeric}:**")
                                fig_hist = px.histogram(df, x=selected_numeric, nbins=30)
                                fig_hist.update_layout(height=400)
                                st.plotly_chart(fig_hist, use_container_width=True)
                            
                            with col2:
                                if len(numeric_cols) > 1:
                                    # Box plot
                                    st.write(f"**Box Plot de {selected_numeric}:**")
                                    fig_box = px.box(df, y=selected_numeric)
                                    fig_box.update_layout(height=400)
                                    st.plotly_chart(fig_box, use_container_width=True)
                            
                            # Matriz de correlación si hay múltiples columnas
                            if len(numeric_cols) > 1:
                                st.write("**Matriz de correlación:**")
                                corr_matrix = df[numeric_cols].corr()
                                fig_corr = px.imshow(corr_matrix, 
                                                   text_auto=True, 
                                                   aspect="auto",
                                                   color_continuous_scale='RdBu')
                                fig_corr.update_layout(height=500)
                                st.plotly_chart(fig_corr, use_container_width=True)
                        
                        # Gráficas categóricas
                        categorical_cols = df.select_dtypes(include=['object', 'string']).columns
                        if len(categorical_cols) > 0:
                            selected_cat = st.selectbox("Columna categórica:", categorical_cols)
                            
                            if df[selected_cat].nunique() <= 20:
                                st.write(f"**Conteo de valores en {selected_cat}:**")
                                value_counts = df[selected_cat].value_counts().head(15)
                                fig_bar = px.bar(x=value_counts.values, y=value_counts.index, orientation='h')
                                fig_bar.update_layout(height=400)
                                st.plotly_chart(fig_bar, use_container_width=True)
                    
                    with tab5:
                        st.write("**🔍 Explorador de datos avanzado:**")
                        
                        # Filtros dinámicos
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            # Filtro por columna
                            filter_col = st.selectbox("Filtrar por columna:", ['Ninguno'] + list(df.columns))
                            
                            if filter_col != 'Ninguno':
                                unique_values = df[filter_col].dropna().unique()
                                if len(unique_values) <= 50:
                                    selected_values = st.multiselect(
                                        f"Valores de {filter_col}:",
                                        options=unique_values,
                                        default=unique_values[:5] if len(unique_values) > 5 else unique_values
                                    )
                                    
                                    if selected_values:
                                        filtered_df = df[df[filter_col].isin(selected_values)]
                                    else:
                                        filtered_df = df
                                else:
                                    st.info("Demasiados valores únicos para filtrar")
                                    filtered_df = df
                            else:
                                filtered_df = df
                        
                        with col2:
                            # Búsqueda de texto
                            search_term = st.text_input("Buscar en todas las columnas:")
                            
                            if search_term:
                                # Buscar en todas las columnas de texto
                                text_cols = df.select_dtypes(include=['object', 'string']).columns
                                mask = pd.Series([False] * len(filtered_df))
                                
                                for col in text_cols:
                                    mask |= filtered_df[col].astype(str).str.contains(search_term, case=False, na=False)
                                
                                if mask.any():
                                    filtered_df = filtered_df[mask]
                                    st.success(f"Encontradas {len(filtered_df)} filas con '{search_term}'")
                                else:
                                    st.warning(f"No se encontraron resultados para '{search_term}'")
                        
                        # Mostrar datos filtrados
                        if len(filtered_df) > 0:
                            st.write(f"**Datos filtrados ({len(filtered_df):,} filas):**")
                            
                            # Paginación para datos filtrados
                            rows_per_page = st.slider("Filas por página:", 10, 100, 25)
                            total_pages = (len(filtered_df) + rows_per_page - 1) // rows_per_page
                            
                            if total_pages > 1:
                                page = st.selectbox(f"Página (1-{total_pages}):", range(1, total_pages + 1))
                                start_idx = (page - 1) * rows_per_page
                                end_idx = start_idx + rows_per_page
                                display_df = filtered_df.iloc[start_idx:end_idx]
                            else:
                                display_df = filtered_df
                            
                            st.dataframe(display_df, use_container_width=True)
                            
                            # Exportar datos filtrados
                            if len(filtered_df) != len(df):
                                csv_filtered = filtered_df.to_csv(index=False)
                                st.download_button(
                                    "📥 Exportar datos filtrados",
                                    data=csv_filtered,
                                    file_name=f"{selected_file['nombre'].split('.')[0]}_filtrado.csv",
                                    mime="text/csv"
                                )
                        else:
                            st.warning("No hay datos que mostrar con los filtros aplicados")
    else:
        st.info("Primero busca archivos para poder leerlos")
    
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