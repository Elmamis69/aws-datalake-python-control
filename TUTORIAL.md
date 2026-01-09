# 🚀 Guía Completa — AWS Data Lake Python Control

Este documento describe cómo operar, monitorear y extender el pipeline event-driven de ingesta y procesamiento de datos en AWS usando solo Python y VS Code.

## ✨ Características principales
- 🔄 Pipeline automático de procesamiento de datos
- 📊 Dashboard web en tiempo real con Streamlit
- 🔍 Monitor de sistema integrado
- 📈 Métricas y visualizaciones
- 🚨 Alertas y notificaciones
- 🛠️ Herramientas de testing y debugging

---

## 1. Requisitos previos
- Python 3.8+
- AWS CLI configurado (`aws configure`)
- Acceso a los buckets S3 y la cola SQS definidos en `config/settings.yaml`
- Instalar dependencias:

```bash
python -m venv .venv
.venv/Scripts/activate  # En Windows
pip install -r requirements.txt
```

---

## 2. Configuración y Verificación

### Configurar el sistema
- Edita `config/settings.yaml` con los nombres de tus buckets, prefijos y cola SQS.
- Asegúrate de tener el perfil de AWS correcto configurado.

### ✅ Verificar que todo funciona (¡NUEVO!)

Antes de usar el sistema, ejecuta el script de diagnóstico:

```bash
python test_app.py
```

Este script verifica:
- ✅ Configuración cargada correctamente
- ✅ Conexión AWS exitosa
- ✅ Glue Catalog funcionando
- ✅ Operaciones S3 funcionando

Si todas las pruebas pasan, el sistema está listo para usar.

---

## 📊 Dashboard Web Avanzado

Lanza el dashboard interactivo para monitorear tu data lake en tiempo real:

```bash
python main.py dashboard
```

### 🎯 **Características del Dashboard:**

#### **Métricas Principales (6 cards):**
- 📁 **Archivos RAW** - Archivos sin procesar + tamaño total
- ✅ **Procesados** - Archivos convertidos a Parquet + tamaño
- 📊 **Total Datos** - Tamaño acumulado de todo el sistema
- 🔥 **Hoy** - Archivos procesados en el día actual
- 📬 **Cola SQS** - Mensajes pendientes de procesar
- 🔴 **Errores (24h)** - Errores recientes del sistema

#### **Análisis Avanzado (3 gráficas):**
- 📈 **Archivos por Tipo** - Distribución de CSV, JSON, Parquet, etc.
- 🍰 **Distribución por Carpetas** - RAW, Procesados, Athena-results
- 📊 **Eficiencia del Sistema** - % de archivos procesados con barra de progreso

#### **Estado del Sistema:**
- 🚦 **Estado General** - Operativo/Problemas/Atención
- 🤖 **Worker Status** - Detecta si el worker está corriendo (PID + tiempo activo)

#### **Lector de Archivos Integrado (¡NUEVO!):**
- 📖 **Selección interactiva** - Elige cualquier archivo de la lista numerada
- 📁 **Todos los tipos** - Parquet, JSON, JSONL, CSV, TXT, Metadata
- ⬇️ **Descarga directa** - Botón para descargar archivos completos
- 📈 **Análisis completo** - 5 pestañas de exploración de datos:
  1. **🔍 Vista Previa** - Primeras/últimas/aleatorias filas
  2. **📊 Estadísticas** - Descripción completa y análisis de nulos
  3. **🏷️ Tipos de Datos** - Análisis detallado por columna
  4. **📈 Gráficas** - Histogramas, correlaciones, box plots
  5. **🔍 Explorar** - Filtros dinámicos y búsqueda de texto
- 📥 **Exportar datos** - CSV para Excel/Google Sheets
- 🔍 **Filtros avanzados** - Por columna y búsqueda de texto

#### **Explorador de Archivos Avanzado:**
- 🔍 **Filtros múltiples:**
  - **Origen:** Procesados / RAW / Todos los buckets
  - **Tipo:** Todos / parquet / jsonl / csv / json / txt / metadata
  - **Fecha:** Filtro opcional por día específico
- 📄 **Tabla optimizada:**
  - Numeración automática (#)
  - Columnas: Archivo, Tipo, Tamaño, Fecha
  - Paginación inteligente (20 archivos por página)
  - Selector de página en esquina inferior derecha

### 🎮 **Cómo usar el Dashboard:**

1. **Monitoreo general:** Las 6 métricas te dan una vista rápida del sistema
2. **Análisis detallado:** Las 3 gráficas muestran distribuciones y eficiencia
3. **Verificar worker:** La sección Worker Status te dice si está corriendo
4. **Explorar archivos:** Usa los filtros para encontrar archivos específicos
5. **Leer archivos:** Selecciona cualquier archivo y usa "Leer Archivo" para análisis completo
6. **Descargar archivos:** Botón "Descargar" para cualquier tipo de archivo
7. **Exportar datos:** Usa "Exportar CSV" para abrir en Excel/Google Sheets
8. **Navegación:** Usa el selector de página para ver más archivos

### ⚙️ **Configuración:**
- **Auto-refresh:** Desactivado por defecto (activa manualmente si quieres)
- **Cache:** 30 segundos para mejor rendimiento
- **Acceso:** http://localhost:8501

---

## 🔍 Monitor del Sistema (¡NUEVO!)

Ejecuta el monitor para obtener métricas detalladas:

```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/run_monitor.py"
```

El monitor te muestra:
- Archivos en buckets S3 (RAW y procesados)
- Mensajes en cola SQS
- Logs de errores recientes
- Métricas de rendimiento

---

## 3. Comandos principales

Usa `main.py` para ejecutar todas las operaciones:

### Worker automático (procesamiento continuo)
```bash
python main.py worker
```
Se queda corriendo, procesando mensajes de SQS automáticamente.

### Dashboard web interactivo
```bash
python main.py dashboard
```
Abre el dashboard en http://localhost:8501

### Probar pipeline end-to-end
```bash
python main.py pipeline
```
Sube archivo de prueba y activa el flujo completo.

### Actualizar catálogo de Glue
```bash
python main.py glue
```
Registra archivos Parquet como tablas SQL.

### Consultar con Athena
```bash
python main.py athena
```
Ejecuta consulta SQL de ejemplo sobre los datos.

### 🔍 Athena Interactivo (¡NUEVO!)
```bash
python main.py athena-sql
```
Consola SQL interactiva para ejecutar consultas personalizadas en tiempo real.

#### **Características del Athena Interactivo:**
- **Consultas multilínea** - Escribe SQL complejo en múltiples líneas
- **Comandos especiales:**
  - `tables` - Ver todas las tablas disponibles
  - `schema TABLA` - Ver columnas y tipos de una tabla
  - `exit` - Salir del programa
- **Resultados en tiempo real** - Ve los datos inmediatamente
- **Formato tabla** - Resultados organizados y fáciles de leer

#### **Cómo usar Athena Interactivo:**

1. **Ejecutar comando:**
   ```bash
   python main.py athena-sql
   ```

2. **Ver tablas disponibles:**
   ```sql
   🔍 SQL> tables
   📋 TABLAS DISPONIBLES:
     • year_2025 (2 columnas) - s3://bucket/processed/events/year=2025/
     • year_2026 (3 columnas) - s3://bucket/processed/events/year=2026/
   ```

3. **Ver esquema de tabla:**
   ```sql
   🔍 SQL> schema year_2026
   📊 ESQUEMA DE TABLA: year_2026
   --------------------------------------------------
     event_time           | bigint
     user_id              | bigint
     action               | string
   ```

4. **Consultas de una línea:**
   ```sql
   🔍 SQL> SELECT COUNT(*) FROM year_2026;
   ✅ Consulta exitosa! (2 filas)
   📊 RESULTADOS:
   ----------------
   _col0
   ----------------
   16
   ```

5. **Consultas multilínea:**
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

#### **Ejemplos de consultas útiles:**
```sql
-- Ver todos los datos
SELECT * FROM year_2026 LIMIT 10;

-- Análisis por usuario
SELECT user_id, COUNT(*) as eventos
FROM year_2026 
GROUP BY user_id 
ORDER BY eventos DESC;

-- Convertir timestamp a fecha legible
SELECT 
    FROM_UNIXTIME(event_time/1000000000) as fecha,
    user_id,
    action
FROM year_2026 
ORDER BY event_time DESC 
LIMIT 5;

-- Actividad por tipo de acción
SELECT action, COUNT(*) as total
FROM year_2026 
GROUP BY action;
```

### Leer archivos desde terminal (¡NUEVO!)
```bash
python main.py read
```
Lector interactivo de archivos con análisis completo de datos.

### Sincronizar con S3
```bash
# Ver todos los archivos
python main.py s3-sync --bucket tu-bucket-name

# Ver archivos de una carpeta específica
python main.py s3-sync --bucket tu-bucket --prefix processed/

# Limitar número de archivos mostrados
python main.py s3-sync --bucket tu-bucket --prefix raw/ --limit 5

# Ver archivos de una fecha específica
python main.py s3-sync --bucket tu-bucket --prefix raw/ --date 2026-01-08

# Ver los últimos N archivos subidos (más recientes)
python main.py s3-sync --bucket tu-bucket --latest 3

# Combinar filtros: últimos 5 archivos de hoy
python main.py s3-sync --bucket tu-bucket --latest 5 --date 2026-01-08
```
Explora archivos en S3 con filtros avanzados por fecha y cantidad.

---

## 📖 Lector de Archivos Interactivo (¡NUEVO!)

Lee y analiza cualquier archivo de tu data lake directamente desde la terminal:

```bash
python main.py read
```

### 🎯 **Características del Lector:**

#### **📁 Selección de archivos:**
- **Lista completa** - Ve todos los archivos disponibles (RAW + Procesados)
- **Tabla organizada** - Número, nombre, tipo, tamaño, origen, ruta completa
- **Selección interactiva** - Escribe el número o presiona ENTER para el más reciente
- **Tipos soportados** - Parquet, JSON, JSONL, CSV, TXT, Metadata

#### **📊 Análisis completo de datos:**
- **Información básica** - Dimensiones, memoria, columnas
- **Vista previa** - Primeras 5 filas con formato limpio
- **Estadísticas numéricas** - Descripción completa (mean, std, min, max, etc.)
- **Información de columnas** - Tipos, nulos, valores únicos
- **Valores categóricos** - Top 5 valores más frecuentes

#### **📄 Archivos de texto y metadata:**
- **Múltiples codificaciones** - UTF-8, Latin-1, ASCII, CP1252
- **Archivos binarios** - Muestra contenido hexadecimal si no es texto
- **Metadata de Athena** - Lee archivos .metadata con información de consultas
- **Truncamiento inteligente** - Limita contenido largo para mejor legibilidad

### 🚀 **Cómo usar:**

1. **Ejecutar comando:**
   ```bash
   python main.py read
   ```

2. **Ver lista de archivos:**
   ```
   📁 ARCHIVOS DISPONIBLES (40):
   #    Archivo                             Tipo       Tamaño     Origen          Ruta Completa
   1    test_20260108_002247.jsonl          JSONL      159B       RAW-Todos       raw/events/incoming/test_20260108_002247.jsonl
   2    metadata.csv                        CSV        1.2KB      RAW-Todos       athena-results/metadata.csv
   3    test.parquet                        PARQUET    2.3KB      Procesados      processed/events/test.parquet
   ```

3. **Seleccionar archivo:**
   ```
   🎯 Selecciona archivo (1-40) o ENTER para el más reciente: 3
   📖 Seleccionado: test.parquet
   ```

4. **Ver análisis completo:**
   ```
   📊 RESUMEN DEL ARCHIVO
   📁 Archivo: test.parquet
   📌 Ruta: processed/events/test.parquet
   📊 Dimensiones: 1,234 filas × 5 columnas
   💾 Memoria: 45.2 KB
   📋 Columnas: event_time, user_id, action, value, category
   
   🔍 VISTA PREVIA (primeras 5 filas)
   [tabla con datos]
   
   📈 ESTADÍSTICAS NUMÉRICAS
   [estadísticas detalladas]
   
   🏷️ INFORMACIÓN DE COLUMNAS
   [tipos, nulos, únicos por columna]
   ```

### 💡 **Ventajas:**
- **Sin configuración** - Funciona inmediatamente
- **Todos los archivos** - Ve archivos de cualquier carpeta (RAW, procesados, athena-results)
- **Análisis instantáneo** - Estadísticas completas sin escribir código
- **Interfaz limpia** - Salida organizada y fácil de leer
- **Manejo robusto** - Soporta diferentes codificaciones y archivos binarios

---

## 4. 🏃 Inicio Rápido - Flujo completo

### Paso 1: Verificar sistema
```bash
python test_app.py
```

### Paso 2: Ejecutar worker (Terminal 1)
```bash
python main.py worker
```

### Paso 3: Dashboard (Terminal 2)
```bash
python main.py dashboard
```

### Paso 4: Probar pipeline (Terminal 3)
```bash
python main.py pipeline
```

¡Verás los archivos procesándose en tiempo real! 🚀

---

## 5. Probar el pipeline end-to-end

Genera y sube un archivo de prueba, y envía el mensaje a SQS:

```bash
python main.py pipeline
```

Esto simula la llegada de un archivo nuevo y activa el flujo completo.

---

## 6. 📁 Explorar archivos S3 (avanzado)

El comando `s3-sync` tiene filtros potentes para encontrar exactamente lo que necesitas:

```bash
# "Muéstrame los últimos 3 archivos que se subieron"
python main.py s3-sync --bucket tu-bucket --latest 3

# "¿Qué archivos llegaron el 8 de enero?"
python main.py s3-sync --bucket tu-bucket --date 2026-01-08

# "Los últimos 5 archivos de hoy en la carpeta RAW"
python main.py s3-sync --bucket tu-bucket --prefix raw/ --latest 5 --date 2026-01-08

# "Solo muéstrame 10 archivos de la carpeta procesados"
python main.py s3-sync --bucket tu-bucket --prefix processed/ --limit 10
```

### **Parámetros disponibles:**
- `--prefix carpeta/` - Filtrar por carpeta
- `--date YYYY-MM-DD` - Filtrar por fecha específica
- `--latest N` - Los N archivos más recientes
- `--limit N` - Máximo N archivos a mostrar

### **Información mostrada:**
- ✅ Ruta completa del archivo
- ✅ Fecha y hora de subida
- ✅ Ordenado por más recientes
- ✅ Contador total de archivos


---

## 7. Leer archivos Parquet desde Python (opcional)

Instala pandas y pyarrow si no los tienes:

```bash
pip install pandas pyarrow
```

Ejemplo de lectura:

```python
import pandas as pd
import s3fs

bucket = "<tu-bucket>"
key = "<ruta-al-archivo.parquet>"

s3_path = f"s3://{bucket}/{key}"
df = pd.read_parquet(s3_path, storage_options={"anon": False})
print(df)
```

---

## 8. Consultar datos con Athena desde Python

Puedes lanzar consultas SQL sobre tus datos procesados en S3 usando Athena y obtener los resultados directamente en Python.

Ejecuta el script de ejemplo:

```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/athena_query_example.py"
```

El script lanza una consulta como:

```sql
SELECT * FROM datalake_processed_db.year_2026 LIMIT 10;
```

Y muestra los resultados en la terminal. Puedes modificar la consulta y la tabla según tus necesidades.

Recuerda que Athena necesita un bucket de resultados (output_location) con permisos de escritura.

---

---

## 9. 🛠️ Herramientas de Debugging

### Verificar sistema completo
```bash
python test_app.py
```

### Comandos principales
```bash
# Worker automático
python main.py worker

# Dashboard web
python main.py dashboard

# Probar pipeline
python main.py pipeline

# Actualizar catálogo
python main.py glue

# Consultar con Athena
python main.py athena

# Athena Interactivo (¡NUEVO!)
python main.py athena-sql

# Leer archivos desde terminal
python main.py read

# Ver archivos S3 (básico)
python main.py s3-sync --bucket tu-bucket

# Ver archivos S3 (avanzado)
python main.py s3-sync --bucket tu-bucket --latest 5 --date 2026-01-08
```

### Monitor puntual (método anterior)
```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/run_monitor.py"
```

### Limpiar cola SQS (si es necesario)
```python
import boto3
sqs = boto3.client('sqs')
sqs.purge_queue(QueueUrl='tu-queue-url')
```

---

## 10. Tips y troubleshooting
- 📊 **Dashboard lento**: Desactiva auto-refresh si tienes muchos archivos
- 🔄 **Worker no procesa**: Revisa credenciales, permisos y formato del mensaje SQS
- 📄 **Logs**: Monitorea `logs/worker.log` para errores detallados
- 🧹 **Limpiar cola**: Usa la consola AWS o boto3 para purgar mensajes
- 📊 **Métricas**: El dashboard guarda cache por 30s para mejor rendimiento

---

## 11. 🚀 Extensiones y mejoras sugeridas
### ✅ COMPLETAMENTE IMPLEMENTADO
- ✅ Dashboard web interactivo con 6 métricas en tiempo real
- ✅ Análisis avanzado con 3 gráficas interactivas
- ✅ Explorador de archivos con filtros múltiples (tipo, fecha, origen)
- ✅ Lector de archivos integrado en dashboard (5 pestañas completas)
- ✅ Lector de archivos interactivo por terminal
- ✅ Descarga de archivos desde dashboard
- ✅ Soporte completo para todos los tipos de archivo (parquet, json, csv, txt, metadata)
- ✅ Análisis completo de datos con estadísticas y gráficas
- ✅ Manejo robusto de codificaciones y archivos binarios
- ✅ Paginación inteligente y numeración
- ✅ Worker status en tiempo real con detección de PID
- ✅ Monitor de sistema integrado
- ✅ Comandos CLI simplificados y unificados
- ✅ Filtros S3 avanzados por fecha y tipo
- ✅ **Athena Interactivo** - Console SQL con consultas multilínea
- ✅ **Comandos especiales** - `tables`, `schema`, navegación intuitiva
- ✅ **Análisis en tiempo real** - Resultados SQL inmediatos

### 🚧 Próximas mejoras sugeridas
- Validación de esquema de datos automática
- Manejo de errores avanzado y Dead Letter Queue (DLQ)
- Notificaciones por email/Slack cuando hay errores
- Automatización completa de Glue/Athena
- Historial de métricas en base de datos
- Tests automatizados y CI/CD
- Alertas proactivas de rendimiento

---

**¡Con esta guía puedes operar, monitorear y probar todo el pipeline sin depender de la consola web de AWS!** 🎉

### 📱 Accesos rápidos
- **Dashboard**: `python main.py dashboard` → http://localhost:8501
- **Lector de archivos**: `python main.py read`
- **Worker**: `python main.py worker`
- **Pipeline**: `python main.py pipeline`
- **Glue**: `python main.py glue`
- **Athena (ejemplo)**: `python main.py athena`
- **🔍 Athena Interactivo**: `python main.py athena-sql`
- **S3 (básico)**: `python main.py s3-sync --bucket tu-bucket`
- **S3 (filtros)**: `python main.py s3-sync --bucket tu-bucket --latest 3`
- **Verificar**: `python test_app.py`
- **Logs**: `logs/worker.log`
- **Config**: `config/settings.yaml`
