# 🚀 AWS Data Lake Python Control

**Pipeline completo de ingesta, procesamiento y análisis de datos en AWS controlado 100% desde Python**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20SQS%20%7C%20Glue%20%7C%20Athena-orange.svg)](https://aws.amazon.com)
[![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-red.svg)](https://streamlit.io)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## ✨ Características Principales

🔄 **Pipeline Automático** - Procesamiento event-driven con SQS  
📊 **Dashboard Web** - Monitoreo en tiempo real con 6 métricas + 3 gráficas  
🔍 **Athena Interactivo** - Consola SQL con consultas multilínea  
📖 **Lector de Archivos** - Análisis completo de datos (terminal + web)  
🎯 **Filtros Avanzados** - Exploración de S3 por fecha, tipo y origen  
⚡ **Worker en Tiempo Real** - Detección automática de estado  
📈 **Análisis Completo** - Estadísticas, gráficas y exportación  
🛠️ **CLI Unificado** - Un solo comando para todo  

## 🏗️ Arquitectura

```
S3 (RAW) → SQS → Python Worker → S3 (Parquet) → Glue Catalog → Athena
    ↓                                    ↓              ↓
Dashboard ←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←
```

## 🚀 Inicio Rápido

### 1. Instalación
```bash
git clone https://github.com/Elmamis69/aws-datalake-python-control.git
cd aws-datalake-python-control
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 2. Configuración
```bash
# Configurar AWS CLI
aws configure

# Editar configuración
# Edita config/settings.yaml con tus buckets y cola SQS
```

### 3. Verificar Sistema
```bash
python test_app.py
```

### 4. ¡Listo! 🎉
```bash
# Dashboard web
python main.py dashboard

# Worker automático
python main.py worker

# Athena interactivo
python main.py athena-sql
```

## 📊 Dashboard Web Avanzado

**Acceso:** http://localhost:8501

### 🎯 Métricas en Tiempo Real (6 cards)
- 📁 **Archivos RAW** - Sin procesar + tamaño total
- ✅ **Procesados** - Convertidos a Parquet + tamaño  
- 📊 **Total Datos** - Tamaño acumulado del sistema
- 🔥 **Hoy** - Archivos procesados hoy
- 📬 **Cola SQS** - Mensajes pendientes
- 🔴 **Errores (24h)** - Errores recientes

### 📈 Análisis Avanzado (3 gráficas)
- **Distribución por Tipo** - CSV, JSON, Parquet, etc.
- **Distribución por Carpetas** - RAW vs Procesados
- **Eficiencia del Sistema** - % procesados con barra de progreso

### 📖 Lector de Archivos Integrado
- **5 pestañas completas:** Vista Previa, Estadísticas, Tipos, Gráficas, Explorar
- **Todos los tipos:** Parquet, JSON, JSONL, CSV, TXT, Metadata
- **Descarga directa** y **exportación CSV**
- **Filtros dinámicos** y búsqueda de texto

## 🔍 Athena Interactivo

**Consola SQL completa con soporte multilínea**

```bash
python main.py athena-sql
```

### Características:
- ✅ **Consultas multilínea** - SQL complejo en múltiples líneas
- ✅ **Comandos especiales** - `tables`, `schema TABLA`, `exit`
- ✅ **Resultados en tiempo real** - Ve datos inmediatamente
- ✅ **Formato tabla** - Salida organizada y legible

### Ejemplo de uso:
```sql
🔍 SQL> SELECT action, COUNT(*) as cantidad
     ... FROM year_2026 
     ... GROUP BY action
     ... ORDER BY cantidad DESC;
✅ Consulta exitosa! (3 filas)
📊 RESULTADOS:
----------------
action    | cantidad
----------------
login     | 8
logout    | 5
view      | 3
```

## 🛠️ Comandos Principales

```bash
# 📊 Dashboard web interactivo
python main.py dashboard

# 🔄 Worker automático (procesamiento continuo)
python main.py worker

# 🔍 Athena interactivo (consultas SQL)
python main.py athena-sql

# 📖 Lector de archivos (terminal)
python main.py read

# 🧪 Probar pipeline completo
python main.py pipeline

# 📋 Actualizar catálogo Glue
python main.py glue

# 📄 Explorar archivos S3
python main.py s3-sync --bucket tu-bucket --latest 5

# ✅ Verificar sistema
python test_app.py
```

## 📁 Exploración Avanzada de S3

**Filtros potentes para encontrar exactamente lo que necesitas:**

```bash
# Ver últimos 3 archivos subidos
python main.py s3-sync --bucket tu-bucket --latest 3

# Archivos de una fecha específica
python main.py s3-sync --bucket tu-bucket --date 2026-01-08

# Combinar filtros: últimos 5 de hoy en carpeta RAW
python main.py s3-sync --bucket tu-bucket --prefix raw/ --latest 5 --date 2026-01-08

# Solo 10 archivos de carpeta procesados
python main.py s3-sync --bucket tu-bucket --prefix processed/ --limit 10
```

## 📖 Lector de Archivos Terminal

**Análisis completo de cualquier archivo desde la terminal:**

```bash
python main.py read
```

### Características:
- 📊 **Análisis automático** - Estadísticas, tipos, nulos, únicos
- 📄 **Todos los formatos** - Parquet, JSON, JSONL, CSV, TXT, Metadata
- 🔍 **Selección interactiva** - Lista numerada de todos los archivos
- 📈 **Vista previa inteligente** - Primeras filas con formato limpio
- 🛡️ **Manejo robusto** - Múltiples codificaciones y archivos binarios

## 🏗️ Estructura del Proyecto

```
aws-datalake-python-control/
├── 📁 config/
│   └── settings.yaml          # Configuración principal
├── 📁 src/
│   ├── datalake/             # Core del pipeline
│   ├── athena_interactive.py # Consola SQL interactiva
│   └── glue_catalog.py       # Gestión de catálogo
├── 📁 dashboard/
│   └── app.py               # Dashboard Streamlit
├── 📁 scripts/
│   ├── test_pipeline.py     # Pruebas del pipeline
│   └── run_monitor.py       # Monitor del sistema
├── 📄 main.py              # CLI unificado
├── 📄 test_app.py          # Verificación del sistema
└── 📚 tutorial.md          # Guía completa
```

## 🎯 Casos de Uso

### 🔄 Procesamiento Automático
1. **Subir archivo** → S3 RAW
2. **Mensaje automático** → SQS
3. **Worker procesa** → Convierte a Parquet
4. **Guarda resultado** → S3 Procesados
5. **Registra en Glue** → Disponible en Athena

### 📊 Análisis de Datos
1. **Dashboard web** → Métricas en tiempo real
2. **Athena interactivo** → Consultas SQL personalizadas
3. **Lector de archivos** → Análisis detallado
4. **Exportación** → CSV para Excel/Google Sheets

### 🔍 Exploración y Debugging
1. **Filtros S3** → Encontrar archivos específicos
2. **Worker status** → Verificar estado del sistema
3. **Logs detallados** → Troubleshooting
4. **Tests automáticos** → Validación del sistema

## 🛡️ Principios de Diseño

- ✅ **Event-driven** - Procesamiento automático basado en eventos
- ✅ **Versionado** - Control de versiones y reproducibilidad
- ✅ **Sin credenciales hardcodeadas** - Configuración externa
- ✅ **AWS como backend** - Aprovecha servicios nativos
- ✅ **Extensible** - Fácil de modificar y extender
- ✅ **Producción-ready** - Manejo robusto de errores

## 📋 Requisitos

- **Python 3.8+**
- **AWS CLI configurado** (`aws configure`)
- **Servicios AWS:** S3, SQS, Glue, Athena
- **Dependencias:** Ver `requirements.txt`

## 🚀 Próximas Mejoras

- 🔄 Validación automática de esquemas
- 📧 Notificaciones por email/Slack
- 🗄️ Historial de métricas en base de datos
- 🧪 Tests automatizados y CI/CD
- ⚠️ Dead Letter Queue (DLQ) para errores
- 📊 Alertas proactivas de rendimiento

## 📚 Documentación

- **[Tutorial Completo](tutorial.md)** - Guía paso a paso
- **[Configuración](config/settings.yaml)** - Parámetros del sistema
- **[Logs](logs/)** - Archivos de registro

## 🤝 Contribuir

1. Fork el repositorio
2. Crea una rama para tu feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit tus cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crea un Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.

---

**¡Con este sistema puedes operar un data lake completo sin depender de la consola web de AWS!** 🎉

### 🔗 Enlaces Rápidos
- **Dashboard:** `python main.py dashboard` → http://localhost:8501
- **Athena SQL:** `python main.py athena-sql`
- **Leer archivos:** `python main.py read`
- **Verificar sistema:** `python test_app.py`
- **Tutorial completo:** [tutorial.md](tutorial.md)