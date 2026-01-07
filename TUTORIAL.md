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

## 2. Configuración
- Edita `config/settings.yaml` con los nombres de tus buckets, prefijos y cola SQS.
- Asegúrate de tener el perfil de AWS correcto configurado.

---

## 📊 Dashboard Web (¡NUEVO!)

Lanza el dashboard interactivo para monitorear tu data lake en tiempo real:

```bash
streamlit run dashboard/app.py
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

## 3. Ejecutar el worker (procesador principal)

Desde la raíz del proyecto:

```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/run_worker.py"
```

El worker quedará escuchando la cola SQS y procesará archivos automáticamente.

💡 **Tip**: Mantén el worker corriendo en una terminal mientras usas el dashboard en otra.

---

## 4. 🏃 Inicio Rápido - Todo en uno

Para probar todo el sistema de una vez:

1. **Terminal 1** - Worker:
```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/run_worker.py"
```

2. **Terminal 2** - Dashboard:
```bash
streamlit run dashboard/app.py
```

3. **Terminal 3** - Test:
```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/test_pipeline.py"
```

¡Verás los archivos procesándose en tiempo real en el dashboard! 🚀

---

## 5. Probar el pipeline end-to-end

Genera y sube un archivo de prueba, y envía el mensaje a SQS:

```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/test_pipeline.py"
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

### Monitor puntual
```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/run_monitor.py"
```

### Verificar configuración
```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/test_config.py"
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
- **Dashboard**: http://localhost:8501
- **Logs**: `logs/worker.log`
- **Config**: `config/settings.yaml`
- **Tests**: `scripts/test_pipeline.py`
