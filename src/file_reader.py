"""
Lector de archivos interactivo para terminal
"""

import pandas as pd
import boto3
import warnings

def run_read_files(config):
    """Leer y mostrar contenido de archivos"""
    # Suprimir warnings de librerías
    warnings.filterwarnings('ignore')
    
    print("\n" + "="*60)
    print("📖 LECTOR DE ARCHIVOS AWS S3")
    print("="*60)
    
    # Verificar pandas
    try:
        print("✅ Pandas disponible")
    except ImportError:
        print("❌ Error: pandas no está instalado")
        print("💡 Ejecuta: pip install pandas")
        return False
    
    try:
        aws_conf = config['aws']
        
        # Verificar credenciales AWS
        try:
            s3 = boto3.client('s3')
            s3.list_buckets()
            print("✅ Conexión AWS establecida")
        except Exception as e:
            print(f"❌ Error AWS: {e}")
            print("💡 Verifica credenciales AWS")
            return False
        
        print("\n🔍 Buscando archivos...")
        
        available_files = []
        
        # Buscar archivos
        for bucket_type, bucket_name, prefix in [
            ('Procesados', aws_conf['s3_processed_bucket'], aws_conf['s3_processed_prefix']),
            ('RAW-Todos', aws_conf['s3_raw_bucket'], '')  # Sin prefijo para buscar TODO
        ]:
            try:
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                for obj in response.get('Contents', []):
                    # Incluir más tipos de archivo
                    if obj['Key'].endswith(('.parquet', '.json', '.jsonl', '.csv', '.txt', '.metadata')):
                        available_files.append({
                            'bucket': bucket_name,
                            'key': obj['Key'],
                            'name': obj['Key'].split('/')[-1],
                            'type': obj['Key'].split('.')[-1] if '.' in obj['Key'] else 'sin_ext',
                            'size': obj['Size'],
                            'modified': obj['LastModified'],
                            'source': bucket_type,
                            'path': obj['Key']  # Ruta completa
                        })
            except Exception:
                continue
        
        if not available_files:
            print("❌ No se encontraron archivos legibles")
            return False
        
        # Ordenar por fecha
        available_files.sort(key=lambda x: x['modified'], reverse=True)
        
        # Mostrar lista de archivos con ruta
        print(f"\n📁 ARCHIVOS DISPONIBLES ({len(available_files)}):")
        print("-" * 160)
        print(f"{'#':<4} {'Archivo':<35} {'Tipo':<10} {'Tamaño':<10} {'Origen':<15} {'Ruta Completa':<80}")
        print("-" * 160)
        
        for i, file in enumerate(available_files, 1):
            # Formato de tamaño mejorado
            size_bytes = file['size']
            if size_bytes >= 1024 * 1024:  # >= 1MB
                size_str = f"{size_bytes / 1024 / 1024:.1f}MB"
            elif size_bytes >= 1024:  # >= 1KB
                size_str = f"{size_bytes / 1024:.1f}KB"
            else:  # < 1KB
                size_str = f"{size_bytes}B"
            
            # Mostrar ruta completa (sin truncar)
            print(f"{i:<4} {file['name'][:34]:<35} {file['type'].upper():<10} {size_str:<10} {file['source']:<15} {file['path']:<80}")
        
        print("-" * 160)
        
        # Selección de archivo
        while True:
            try:
                choice = input(f"\n🎯 Selecciona archivo (1-{len(available_files)}) o ENTER para el más reciente: ").strip()
                
                if choice == "":
                    selected_file = available_files[0]
                    print(f"📖 Seleccionado: {selected_file['name']} (más reciente)")
                    break
                
                choice_num = int(choice)
                if 1 <= choice_num <= len(available_files):
                    selected_file = available_files[choice_num - 1]
                    print(f"📖 Seleccionado: {selected_file['name']}")
                    break
                else:
                    print(f"❌ Número inválido. Usa 1-{len(available_files)}")
            except ValueError:
                print("❌ Ingresa un número válido")
            except KeyboardInterrupt:
                print("\n👋 Cancelado por el usuario")
                return True
        
        # Leer archivo
        print(f"\n⏳ Leyendo {selected_file['name']}...")
        success = read_and_display_file(selected_file)
        
        return success
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def read_and_display_file(selected_file):
    """Leer y mostrar archivo específico"""
    s3_path = f"s3://{selected_file['bucket']}/{selected_file['key']}"
    
    try:
        if selected_file['type'] == 'parquet':
            df = pd.read_parquet(s3_path)
        elif selected_file['type'] in ['json', 'jsonl']:
            df = pd.read_json(s3_path, lines=True if selected_file['type'] == 'jsonl' else False)
        elif selected_file['type'] == 'csv':
            df = pd.read_csv(s3_path)
        elif selected_file['type'] in ['txt', 'metadata']:
            return read_text_file(selected_file)
        else:
            print(f"❌ Tipo de archivo no soportado para análisis: {selected_file['type']}")
            print("💡 Tipos soportados: parquet, json, jsonl, csv, txt, metadata")
            return False
    except Exception as e:
        print(f"❌ Error leyendo archivo: {e}")
        return False
    
    # Mostrar análisis de DataFrame
    display_dataframe_analysis(selected_file, df)
    return True

def read_text_file(selected_file):
    """Leer archivos de texto con manejo de codificación"""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=selected_file['bucket'], Key=selected_file['key'])
        
        # Intentar diferentes codificaciones
        raw_content = response['Body'].read()
        content = None
        
        for encoding in ['utf-8', 'latin-1', 'ascii', 'cp1252']:
            try:
                content = raw_content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            # Si no se puede decodificar, mostrar como hexadecimal
            content = f"Archivo binario - Primeros 500 bytes en hex:\n{raw_content[:500].hex()}"
            encoding_used = "binario"
        else:
            encoding_used = encoding
        
        print("\n" + "="*70)
        print(f"📄 CONTENIDO DEL ARCHIVO: {selected_file['name']}")
        print("="*70)
        print(f"📁 Archivo: {selected_file['name']}")
        print(f"📌 Ruta: {selected_file['path']}")
        print(f"💾 Tamaño: {len(raw_content)} bytes")
        print(f"🔤 Codificación: {encoding_used}")
        print("\n" + "-"*70)
        print("🔍 CONTENIDO:")
        print("-"*70)
        
        # Limitar contenido si es muy largo
        if len(content) > 2000:
            print(content[:2000])
            print(f"\n... (contenido truncado, mostrando primeros 2000 caracteres de {len(content)} totales)")
        else:
            print(content)
        
        print("\n" + "="*70)
        print("✅ LECTURA COMPLETADA")
        print("💡 Archivo de metadata de Athena - contiene información sobre la consulta")
        print("="*70)
        return True
        
    except Exception as e:
        print(f"❌ Error leyendo archivo de texto: {e}")
        return False

def display_dataframe_analysis(selected_file, df):
    """Mostrar análisis completo del DataFrame"""
    # Mostrar resultados
    print("\n" + "="*60)
    print("📊 RESUMEN DEL ARCHIVO")
    print("="*60)
    print(f"📁 Archivo: {selected_file['name']}")
    print(f"📌 Ruta: {selected_file['path']}")
    print(f"📊 Dimensiones: {df.shape[0]:,} filas × {df.shape[1]} columnas")
    
    # Memoria en KB
    memory_kb = df.memory_usage(deep=True).sum() / 1024
    print(f"💾 Memoria: {memory_kb:.1f} KB")
    
    print(f"📋 Columnas: {', '.join(df.columns)}")
    
    # Vista previa
    print("\n" + "-"*60)
    print("🔍 VISTA PREVIA (primeras 5 filas)")
    print("-"*60)
    print(df.head().to_string(index=False))
    
    # Estadísticas numéricas
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        print("\n" + "-"*60)
        print("📈 ESTADÍSTICAS NUMÉRICAS")
        print("-"*60)
        print(df[numeric_cols].describe().to_string())
    
    # Información de columnas
    print("\n" + "-"*60)
    print("🏷️ INFORMACIÓN DE COLUMNAS")
    print("-"*60)
    for col in df.columns:
        dtype = str(df[col].dtype)
        non_null = df[col].count()
        null_count = df[col].isnull().sum()
        unique_count = df[col].nunique()
        
        print(f"{col:<20} {dtype:<15} {non_null:>8,} no-nulos  {null_count:>6,} nulos  {unique_count:>8,} únicos")
    
    # Valores categóricos
    categorical_cols = df.select_dtypes(include=['object', 'string']).columns
    if len(categorical_cols) > 0:
        print("\n" + "-"*60)
        print("🔤 VALORES CATEGÓRICOS (top 5)")
        print("-"*60)
        for col in categorical_cols[:3]:
            if df[col].nunique() <= 20:
                values = df[col].value_counts().head(5)
                print(f"\n{col}:")
                for val, count in values.items():
                    print(f"  {val}: {count:,}")
    
    print("\n" + "="*60)
    print("✅ LECTURA COMPLETADA")
    print("💡 Usa 'python main.py dashboard' para análisis interactivo")
    print("="*60)