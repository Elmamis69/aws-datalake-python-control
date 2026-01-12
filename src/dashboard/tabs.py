"""
Módulo para renderizar pestañas del dashboard
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta
from .file_handler import get_files_advanced_filter, read_file_from_s3, download_file_from_s3
from .sqs_handler import get_sqs_messages_for_dashboard


def render_files_tab(metrics):
    """Renderizar pestaña de archivos"""
    st.subheader("📄 Lista de Archivos")
    
    # Controles de filtro
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
        selected_type = st.selectbox("Tipo de archivo:", options=available_types, index=0)
    
    with col3:
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
    
    # Obtener archivos filtrados
    filtered_files = get_files_advanced_filter(
        metrics['aws_conf'], 
        selected_date, 
        selected_type.lower() if selected_type.lower() != 'todos' else None,
        selected_source
    )
    
    # Mostrar resultados
    if filtered_files:
        st.success(f"📁 **{len(filtered_files)} archivos encontrados**")
        
        # Paginación
        items_per_page = 20
        total_pages = (len(filtered_files) + items_per_page - 1) // items_per_page
        current_page = st.session_state.get('page', 1)
        start_idx = (current_page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        display_files = filtered_files[start_idx:end_idx]
        
        if total_pages > 1:
            st.info(f"Mostrando archivos {start_idx + 1}-{min(end_idx, len(filtered_files))} de {len(filtered_files)} (Página {current_page} de {total_pages})")
        
        # Preparar DataFrame
        for i, file_info in enumerate(display_files):
            file_info['#'] = start_idx + i + 1
        
        df_filtered = pd.DataFrame(display_files)
        columns_order = ['#', 'nombre', 'tipo', 'tamaño', 'fecha']
        df_display = df_filtered[columns_order]
        df_display['📜'] = ['✅' if f['tipo'] in ['parquet', 'json', 'jsonl', 'csv'] else '❌' for f in display_files]
        
        # Mostrar tabla
        st.dataframe(
            df_display,
            column_config={
                "#": st.column_config.NumberColumn("#", help="Número de archivo", width=60),
                "nombre": st.column_config.TextColumn("Archivo", help="Nombre del archivo", width=None),
                "tipo": st.column_config.TextColumn("Tipo", help="Extensión del archivo", width=80),
                "tamaño": st.column_config.TextColumn("Tamaño", help="Tamaño del archivo", width=80),
                "fecha": st.column_config.TextColumn("Fecha", help="Fecha de creación/modificación", width=150),
                "📜": st.column_config.TextColumn("Legible", help="✅ = Se puede leer y analizar, ❌ = Solo descarga", width=70)
            },
            hide_index=True,
            width="stretch"
        )
        
        # Selector de página
        if total_pages > 1:
            col1, col2 = st.columns([3, 1])
            with col2:
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
    
    col1, col2 = st.columns([1, 3])
    with col1:
        reader_source = st.selectbox(
            "Origen para lector:",
            options=['processed', 'raw', 'all'],
            format_func=lambda x: {'processed': 'Solo Procesados', 'raw': 'Solo Raw', 'all': 'Todos los archivos'}[x],
            index=2,
            key='reader_source'
        )
    
    reader_files = get_files_advanced_filter(metrics['aws_conf'], None, None, reader_source)
    
    if reader_files:
        _render_file_reader(reader_files)
    else:
        st.info("No hay archivos disponibles para leer")


def _render_file_reader(reader_files):
    """Renderizar lector de archivos"""
    reader_items_per_page = 10
    reader_total_pages = (len(reader_files) + reader_items_per_page - 1) // reader_items_per_page
    
    reader_current_page = st.session_state.get('reader_page', 1)
    reader_start_idx = (reader_current_page - 1) * reader_items_per_page
    reader_end_idx = reader_start_idx + reader_items_per_page
    reader_display_files = reader_files[reader_start_idx:reader_end_idx]
    
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    with col1:
        file_options = {}
        for i, file_info in enumerate(reader_display_files):
            display_name = f"{reader_start_idx + i + 1:2d}. {file_info['nombre']} ({file_info['tipo'].upper()}, {file_info['tamaño']})"
            file_options[display_name] = file_info
        
        selected_file_display = st.selectbox(
            f"Selecciona un archivo ({len(reader_files)} disponibles):",
            options=list(file_options.keys()),
            index=0 if file_options else None
        )
    
    with col2:
        if reader_total_pages > 1:
            new_reader_page = st.selectbox(
                f"Página:",
                options=list(range(1, reader_total_pages + 1)),
                index=reader_current_page - 1,
                format_func=lambda x: f"{x}/{reader_total_pages}",
                key='reader_page_selector'
            )
            if new_reader_page != reader_current_page:
                st.session_state.reader_page = new_reader_page
                st.rerun()
    
    with col3:
        if selected_file_display:
            selected_file = file_options[selected_file_display]
            is_readable = selected_file['tipo'] in ['parquet', 'json', 'jsonl', 'csv']
            
            read_button = st.button(
                "📖 Leer Archivo", 
                type="primary",
                disabled=not is_readable,
                help="Solo archivos parquet, json, jsonl, csv" if not is_readable else "Leer y analizar archivo"
            )
    
    with col4:
        if selected_file_display:
            download_button = st.button("⬇️ Descargar", type="secondary", help="Descargar archivo completo")
    
    if reader_total_pages > 1:
        st.info(f"Mostrando archivos {reader_start_idx + 1}-{min(reader_end_idx, len(reader_files))} de {len(reader_files)} (Página {reader_current_page} de {reader_total_pages})")
    
    # Procesar acciones
    if selected_file_display:
        selected_file = file_options[selected_file_display]
        st.info(f"📁 **Archivo:** {selected_file['nombre']} | **Tipo:** {selected_file['tipo'].upper()} | **Tamaño:** {selected_file['tamaño']} | **Fecha:** {selected_file['fecha']}")
        
        if download_button:
            _handle_file_download(selected_file)
        
        if read_button and is_readable:
            _handle_file_read(selected_file)


def _handle_file_download(selected_file):
    """Manejar descarga de archivo"""
    with st.spinner(f"Descargando {selected_file['nombre']}..."):
        file_content, error = download_file_from_s3(selected_file['bucket'], selected_file['ruta'])
    
    if error:
        st.error(f"❌ {error}")
    else:
        st.success(f"✅ Archivo descargado: **{selected_file['nombre']}**")
        st.download_button(
            label="💾 Guardar archivo",
            data=file_content,
            file_name=selected_file['nombre'],
            mime="application/octet-stream"
        )


def _handle_file_read(selected_file):
    """Manejar lectura de archivo"""
    with st.spinner(f"Leyendo {selected_file['nombre']}..."):
        df, error = read_file_from_s3(selected_file['bucket'], selected_file['ruta'], selected_file['tipo'])
    
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
            memory_bytes = df.memory_usage(deep=True).sum()
            if memory_bytes < 1024:
                memory_str = f"{memory_bytes} B"
            elif memory_bytes < 1024 * 1024:
                memory_str = f"{memory_bytes / 1024:.1f} KB"
            else:
                memory_str = f"{memory_bytes / 1024 / 1024:.1f} MB"
            st.metric("💾 RAM", memory_str, help="Memoria usada por los datos en RAM")
        with col4:
            csv_data = df.to_csv(index=False)
            st.download_button(
                "🔗 Exportar CSV",
                data=csv_data,
                file_name=f"{selected_file['nombre'].split('.')[0]}_export.csv",
                mime="text/csv",
                help="Descargar como CSV para abrir en Excel/Google Sheets"
            )
        
        st.write("**Vista previa (primeras 10 filas):**")
        st.dataframe(df.head(10), use_container_width=True)


def render_sqs_tab(metrics):
    """Renderizar pestaña de SQS"""
    st.subheader("📬 Mensajes SQS")
    
    # Botón para refrescar mensajes
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Refrescar SQS", help="Obtener mensajes frescos de SQS"):
            st.cache_data.clear()
            st.rerun()
    
    sqs_data = get_sqs_messages_for_dashboard(metrics['aws_conf']['sqs_queue_url'], max_messages=20)
    
    if 'error' in sqs_data:
        st.error(f"❌ Error obteniendo mensajes SQS: {sqs_data['error']}")
        return
    
    # Estado de la cola
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📬 Disponibles", sqs_data['total_messages'])
    with col2:
        st.metric("⏳ En procesamiento", sqs_data['messages_in_flight'])
    with col3:
        st.metric("⏰ Retrasados", sqs_data['messages_delayed'])
    
    # Explicación del visibility timeout
    if sqs_data['messages_in_flight'] > 0:
        st.info(f"💡 Los {sqs_data['messages_in_flight']} mensajes en procesamiento están en visibility timeout (30s) - Usa el botón Refrescar para intentar verlos")
    
    if sqs_data['total_messages'] == 0 and sqs_data['messages_in_flight'] == 0:
        st.success("✅ **No hay mensajes en cola**")
        st.info("📬 La cola SQS está vacía - No hay archivos pendientes de procesar")
    elif sqs_data['total_messages'] == 0 and sqs_data['messages_in_flight'] > 0:
        st.warning(f"⏳ **No hay mensajes disponibles para leer**")
        st.info(f"Hay {sqs_data['messages_in_flight']} mensajes siendo procesados")
    elif sqs_data['total_messages'] > 0:
        st.warning(f"⚠️ **{sqs_data['total_messages']} mensajes en cola**")
        
        worker = metrics['worker_status']
        if worker['running']:
            st.success(f"🤖 Worker activo - Procesando mensajes ({worker['since']})")
        elif sqs_data['total_messages'] > 0 or sqs_data['messages_in_flight'] > 0:
            st.error("🛑 Worker detenido - Los mensajes no se procesarán")
            st.code("python main.py worker")
        else:
            st.info("🔄 Los mensajes pueden estar siendo procesados por el worker")
        
        messages = sqs_data['messages']
        if messages:
            st.markdown("---")
            st.write(f"📋 **Mensajes en Cola ({len(messages)} de {sqs_data['total_messages']}):**")
            
            for i, msg in enumerate(messages, 1):
                with st.expander(f"📨 Mensaje {i} - ID: {msg['message_id'][:8]}...", expanded=i <= 3):
                    col1, col2 = st.columns([1, 1])
                    
                    with col1:
                        st.write(f"**ID completo:** `{msg['message_id']}`")
                        if msg['sent_time']:
                            st.write(f"**Enviado:** {msg['sent_time'].strftime('%Y-%m-%d %H:%M:%S')}")
                        st.write(f"**Recibido:** {msg['receive_count']} veces")
                    
                    with col2:
                        if isinstance(msg['body'], dict):
                            st.write("**Tipo:** JSON")
                            if 'Records' in msg['body']:
                                records = msg['body']['Records']
                                st.write(f"**Records:** {len(records)} eventos")
                                if records:
                                    first_record = records[0]
                                    if 's3' in first_record:
                                        bucket = first_record['s3']['bucket']['name']
                                        key = first_record['s3']['object']['key']
                                        st.write(f"**Archivo:** `{key}`")
                                        st.write(f"**Bucket:** `{bucket}`")
                            else:
                                keys = list(msg['body'].keys())[:5]
                                st.write(f"**Keys:** {keys}")
                        else:
                            st.write("**Tipo:** Texto")
                            content = str(msg['body'])[:100]
                            if len(str(msg['body'])) > 100:
                                content += "..."
                            st.write(f"**Contenido:** `{content}`")
                    
                    if st.checkbox(f"Ver JSON completo - Mensaje {i}", key=f"json_{i}"):
                        try:
                            if isinstance(msg['body'], dict):
                                st.json(msg['body'])
                            else:
                                st.code(msg['raw_body'], language='text')
                        except Exception as e:
                            st.error(f"Error mostrando contenido: {e}")
                            st.text(f"Contenido raw: {msg['raw_body']}")
            
            if len(messages) < sqs_data['total_messages']:
                st.info(f"💡 Hay {sqs_data['total_messages'] - len(messages)} mensajes más en cola")
        else:
            st.info("🔄 Los mensajes pueden estar siendo procesados por el worker")
    elif sqs_data['messages_in_flight'] > 0:
        st.warning("⏳ **Todos los mensajes están en procesamiento**")
        st.info("Los mensajes están en visibility timeout. Espera 30 segundos o usa Refrescar.")