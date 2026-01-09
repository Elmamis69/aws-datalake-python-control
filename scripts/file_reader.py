"""
Lector de Archivos Interactivo - Terminal
Lee y analiza cualquier archivo del data lake desde la terminal
"""

import boto3
import pandas as pd
import json
import yaml
import os
from pathlib import Path
from datetime import datetime

def load_config():
    """Cargar configuración"""
    config_path = Path(__file__).parent.parent / "config" / "settings.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def list_all_files(config):
    """Listar todos los archivos disponibles"""
    s3 = boto3.client('s3', region_name=config['aws'].get('region', 'us-east-2'))
    
    all_files = []
    
    # Buckets a revisar
    buckets_to_check = [
        (config['aws']['s3_raw_bucket'], 'RAW-Todos'),
        (config['aws']['s3_processed_bucket'], 'Procesados')
    ]
    
    for bucket_name, origen in buckets_to_check:
        try:
            response = s3.list_objects_v2(Bucket=bucket_name)
            objects = response.get('Contents', [])
            
            for obj in objects:
                key = obj['Key']
                size = obj['Size']
                last_modified = obj['LastModified']
                
                # Determinar tipo de archivo
                if key.endswith('.parquet'):
                    file_type = 'PARQUET'
                elif key.endswith('.json'):
                    file_type = 'JSON'
                elif key.endswith('.jsonl'):
                    file_type = 'JSONL'
                elif key.endswith('.csv'):
                    file_type = 'CSV'
                elif key.endswith('.txt'):
                    file_type = 'TXT'
                elif key.endswith('.metadata'):
                    file_type = 'METADATA'
                else:
                    file_type = 'OTRO'
                
                # Formatear tamaño
                if size < 1024:
                    size_str = f"{size}B"
                elif size < 1024 * 1024:
                    size_str = f"{size/1024:.1f}KB"
                else:
                    size_str = f"{size/(1024*1024):.1f}MB"
                
                all_files.append({
                    'bucket': bucket_name,
                    'key': key,
                    'nombre': os.path.basename(key),
                    'tipo': file_type,
                    'tamaño': size_str,
                    'fecha': last_modified.strftime('%Y-%m-%d %H:%M'),
                    'origen': origen,
                    'ruta_completa': key
                })
                
        except Exception as e:
            print(f"⚠️ Error accediendo bucket {bucket_name}: {e}")
    
    # Ordenar por fecha (más recientes primero)
    all_files.sort(key=lambda x: x['fecha'], reverse=True)
    
    return all_files

def format_size(size_bytes):
    """Formatear tamaño de archivo"""
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.1f}KB"
    else:
        return f"{size_bytes/(1024*1024):.1f}MB"

def read_file_from_s3(bucket, key, file_type):
    """Leer archivo desde S3"""
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        
        if file_type == 'PARQUET':
            # Para parquet, usar pandas directamente
            df = pd.read_parquet(f's3://{bucket}/{key}')
            return df, None
            
        elif file_type in ['JSON', 'JSONL']:
            # Intentar decodificar como texto
            try:
                text_content = content.decode('utf-8')
                if file_type == 'JSON':
                    data = json.loads(text_content)
                    df = pd.json_normalize(data) if isinstance(data, list) else pd.DataFrame([data])
                else:  # JSONL
                    lines = [json.loads(line) for line in text_content.strip().split('\n') if line.strip()]
                    df = pd.DataFrame(lines)
                return df, None
            except Exception as e:
                return None, f"Error leyendo JSON: {e}"
                
        elif file_type == 'CSV':
            try:
                text_content = content.decode('utf-8')
                from io import StringIO
                df = pd.read_csv(StringIO(text_content))
                return df, None
            except Exception as e:
                return None, f"Error leyendo CSV: {e}"
                
        else:  # TXT, METADATA, OTRO
            # Intentar múltiples codificaciones
            for encoding in ['utf-8', 'latin-1', 'ascii', 'cp1252']:
                try:
                    text_content = content.decode(encoding)
                    return text_content, None
                except:
                    continue
            
            # Si no se puede decodificar, mostrar como hexadecimal
            hex_content = content[:500].hex()  # Primeros 500 bytes
            return f"Archivo binario (hex): {hex_content}...", None
            
    except Exception as e:
        return None, f"Error accediendo archivo: {e}"

def analyze_dataframe(df, filename):
    """Analizar DataFrame y mostrar estadísticas"""
    print(f"\n📊 RESUMEN DEL ARCHIVO")
    print(f"📁 Archivo: {filename}")
    print(f"📊 Dimensiones: {df.shape[0]:,} filas × {df.shape[1]} columnas")
    
    # Memoria
    memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"💾 Memoria: {memory_mb:.1f} MB")
    
    # Columnas
    print(f"📋 Columnas: {', '.join(df.columns.tolist())}")
    
    print(f"\n🔍 VISTA PREVIA (primeras 5 filas)")
    print(df.head().to_string(index=False))
    
    # Estadísticas numéricas
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        print(f"\n📈 ESTADÍSTICAS NUMÉRICAS")
        print(df[numeric_cols].describe().to_string())
    
    # Información de columnas
    print(f"\n🏷️ INFORMACIÓN DE COLUMNAS")
    for col in df.columns:
        dtype = str(df[col].dtype)
        nulls = df[col].isnull().sum()
        null_pct = (nulls / len(df)) * 100
        unique_count = df[col].nunique()
        
        print(f"  {col:20} | {dtype:10} | Nulos: {nulls:,} ({null_pct:.1f}%) | Únicos: {unique_count:,}")
        
        # Mostrar valores más frecuentes para columnas categóricas
        if dtype == 'object' and unique_count <= 10:
            top_values = df[col].value_counts().head(5)
            print(f"    Top valores: {dict(top_values)}")

def run_file_reader():
    """Ejecutar lector de archivos interactivo"""
    print("🚀 LECTOR DE ARCHIVOS INTERACTIVO")
    print("=" * 50)
    
    # Cargar configuración
    config = load_config()
    
    # Listar archivos
    print("📁 Cargando lista de archivos...")
    files = list_all_files(config)
    
    if not files:
        print("❌ No se encontraron archivos en los buckets configurados")
        return
    
    print(f"\n📁 ARCHIVOS DISPONIBLES ({len(files)}):")
    print(f"{'#':>3} {'Archivo':35} {'Tipo':8} {'Tamaño':10} {'Origen':15} {'Ruta Completa'}")
    print("-" * 100)
    
    for i, file_info in enumerate(files, 1):
        print(f"{i:>3} {file_info['nombre']:35} {file_info['tipo']:8} {file_info['tamaño']:10} {file_info['origen']:15} {file_info['ruta_completa']}")
    
    # Selección de archivo
    while True:
        try:
            selection = input(f"\n🎯 Selecciona archivo (1-{len(files)}) o ENTER para el más reciente: ").strip()
            
            if not selection:
                selected_file = files[0]  # Más reciente
                print(f"📖 Seleccionado: {selected_file['nombre']}")
                break
            else:
                index = int(selection) - 1
                if 0 <= index < len(files):
                    selected_file = files[index]
                    print(f"📖 Seleccionado: {selected_file['nombre']}")
                    break
                else:
                    print(f"❌ Número inválido. Debe ser entre 1 y {len(files)}")
                    
        except ValueError:
            print("❌ Por favor ingresa un número válido")
        except KeyboardInterrupt:
            print("\n👋 ¡Hasta luego!")
            return
    
    # Leer archivo
    print(f"\n📖 Leyendo archivo: {selected_file['nombre']}")
    print("⏳ Procesando...")
    
    result, error = read_file_from_s3(
        selected_file['bucket'],
        selected_file['key'],
        selected_file['tipo']
    )
    
    if error:
        print(f"❌ {error}")
        return
    
    # Mostrar resultados
    if isinstance(result, pd.DataFrame):
        analyze_dataframe(result, selected_file['nombre'])
    else:
        # Archivo de texto
        print(f"\n📄 CONTENIDO DEL ARCHIVO: {selected_file['nombre']}")
        print("-" * 50)
        
        # Truncar si es muy largo
        if len(result) > 2000:
            print(result[:2000])
            print(f"\n... (archivo truncado, mostrando primeros 2000 caracteres de {len(result)} totales)")
        else:
            print(result)

if __name__ == "__main__":
    run_file_reader()