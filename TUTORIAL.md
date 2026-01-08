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

## 📊 Dashboard Web

Lanza el dashboard interactivo para monitorear tu data lake en tiempo real:

```bash
python main.py dashboard
```

El dashboard incluye:
- 📁 Conteo de archivos RAW y procesados
- 📬 Estado de la cola SQS
- 🚨 Errores recientes (últimas 24h)
- 📈 Gráficas de tendencias
- 🔄 Auto-refresh cada 30 segundos
- 🚦 Estado general del sistema

Accede en: http://localhost:8501

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

### Sincronizar con S3
```bash
python main.py s3-sync --bucket tu-bucket-name
python main.py s3-sync --bucket tu-bucket --prefix processed/
```
Lista archivos en S3 con filtros opcionales.

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

## 6. Listar archivos procesados en S3 desde Python

Para ver los archivos Parquet generados en la ruta de salida:

```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/list_s3_processed.py"
```

Puedes modificar la fecha en el script para buscar otros días.

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

# Ver archivos S3
python main.py s3-sync --bucket tu-bucket
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
### Implementadas ✅
- ✅ Dashboard web interactivo
- ✅ Monitor de sistema en tiempo real
- ✅ Métricas y visualizaciones
- ✅ Auto-refresh y alertas visuales

### Por implementar 🚧
- Validación de esquema de datos
- Manejo de errores avanzado y DLQ
- Notificaciones por email/Slack
- Automatización de Glue/Athena
- Historial de métricas en base de datos
- Tests automatizados

---

**¡Con esta guía puedes operar, monitorear y probar todo el pipeline sin depender de la consola web de AWS!** 🎉

### 📱 Accesos rápidos
- **Dashboard**: `python main.py dashboard` → http://localhost:8501
- **Worker**: `python main.py worker`
- **Pipeline**: `python main.py pipeline`
- **Glue**: `python main.py glue`
- **Athena**: `python main.py athena`
- **Verificar**: `python test_app.py`
- **Logs**: `logs/worker.log`
- **Config**: `config/settings.yaml`
