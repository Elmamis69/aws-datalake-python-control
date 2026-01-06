# Guía de Uso — AWS Data Lake Python Control

Este documento describe cómo operar, probar y extender el pipeline event-driven de ingesta y procesamiento de datos en AWS usando solo Python y VS Code.

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

## 3. Ejecutar el worker (procesador principal)

Desde la raíz del proyecto:

```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/run_worker.py"
```

El worker quedará escuchando la cola SQS y procesará archivos automáticamente.

---

## 4. Probar el pipeline end-to-end

Genera y sube un archivo de prueba, y envía el mensaje a SQS:

```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/test_pipeline.py"
```

Esto simula la llegada de un archivo nuevo y activa el flujo completo.

---

## 5. Listar archivos procesados en S3 desde Python

Para ver los archivos Parquet generados en la ruta de salida:

```powershell
$env:PYTHONPATH="src"; & ".venv/Scripts/python.exe" "scripts/list_s3_processed.py"
```

Puedes modificar la fecha en el script para buscar otros días.

---

## 6. Leer archivos Parquet desde Python (opcional)

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

## 7. Consultar datos con Athena desde Python

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

## 8. Tips y troubleshooting
- Si el worker no procesa mensajes, revisa credenciales, permisos y formato del mensaje SQS.
- Puedes monitorear logs en `logs/worker.log`.
- Si necesitas limpiar la cola SQS, hazlo desde la consola AWS o con boto3.
- Para agregar nuevas pruebas, edita `scripts/test_pipeline.py`.

---

## 9. Extensión y mejoras sugeridas
- Validación de esquema de datos
- Manejo de errores avanzado y DLQ
- Métricas y monitoreo
- Automatización de Glue/Athena

---

**¡Con esta guía puedes operar y probar todo el pipeline sin depender de la consola web de AWS!**
