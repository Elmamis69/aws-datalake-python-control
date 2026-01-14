"""
Dashboard de Data Lake con Streamlit
¡Rifado y en tiempo real! 🚀
"""

import streamlit as st
import sys
import os
from datetime import datetime
import time

# Agregar el directorio padre al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar módulos del dashboard
from src.dashboard.metrics import get_metrics
from src.dashboard.ui_components import render_metrics_cards, render_system_status, render_advanced_charts
from src.dashboard.tabs import render_files_tab, render_sqs_tab
from src.dashboard.utils import format_size, check_worker_status
from src.dashboard.cloudwatch_dashboard import render_cloudwatch_metrics
from scripts.run_monitor import load_config

# Configurar página
st.set_page_config(
    page_title="Data Lake Dashboard",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)



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
    config_path = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
    config = load_config(config_path)
    metrics = get_metrics(config)
    
    if not metrics:
        st.error("❌ No se pudieron obtener las métricas")
        return
    
    # Renderizar componentes principales
    render_metrics_cards(metrics)
    render_system_status(metrics)
    render_advanced_charts(metrics)
    
    # Crear pestañas principales
    tab1, tab2, tab3 = st.tabs(["📄 Lista de Archivos", "📬 Mensajes SQS", "📊 CloudWatch"])
    
    with tab1:
        render_files_tab(metrics)
    
    with tab2:
        render_sqs_tab(metrics)
    
    with tab3:
        render_cloudwatch_metrics()
    
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