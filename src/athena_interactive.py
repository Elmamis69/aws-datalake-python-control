"""
Athena Interactive - Ejecutar consultas SQL interactivas
Permite escribir cualquier consulta SQL desde la terminal
"""

import boto3
import yaml
import os
import time
from pathlib import Path

def load_config():
    """Cargar configuración"""
    config_path = Path(__file__).parent.parent / "config" / "settings.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def execute_athena_query(query: str, config: dict):
    """Ejecutar consulta en Athena"""
    region = config['aws'].get('region', 'us-east-2')
    database = config['glue']['database_name']
    output_bucket = config['aws']['s3_processed_bucket']
    output_prefix = 'athena-results/'
    output_location = f's3://{output_bucket}/{output_prefix}'
    
    athena = boto3.client('athena', region_name=region)
    
    print(f"🔍 Ejecutando consulta: {query}")
    print("⏳ Procesando...")
    
    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Esperar resultado
        while True:
            result = athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = result['QueryExecution']['Status']['State']
            
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)
        
        if state == 'SUCCEEDED':
            # Obtener y mostrar resultados
            result = athena.get_query_results(QueryExecutionId=query_execution_id)
            rows = result['ResultSet']['Rows']
            
            if not rows:
                print("❌ No se encontraron resultados")
                return
            
            print(f"✅ Consulta exitosa! ({len(rows)} filas)")
            print("📊 RESULTADOS:")
            print("-" * 80)
            
            # Mostrar resultados en formato tabla
            for i, row in enumerate(rows[:20]):  # Limitar a 20 filas
                values = [col.get('VarCharValue', 'NULL') for col in row['Data']]
                if i == 0:
                    # Header
                    print(" | ".join(f"{val:15}" for val in values))
                    print("-" * 80)
                else:
                    # Data rows
                    print(" | ".join(f"{val:15}" for val in values))
            
            if len(rows) > 21:  # 20 data rows + 1 header
                print(f"... y {len(rows) - 21} filas más")
                
        else:
            error_info = result['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido')
            print(f"❌ Error en consulta: {error_info}")
            
    except Exception as e:
        print(f"❌ Error ejecutando consulta: {e}")

def list_available_tables(config: dict):
    """Listar tablas disponibles"""
    try:
        glue = boto3.client('glue', region_name=config['aws'].get('region', 'us-east-2'))
        database = config['glue']['database_name']
        
        response = glue.get_tables(DatabaseName=database)
        tables = response.get('TableList', [])
        
        if tables:
            print("📋 TABLAS DISPONIBLES:")
            for table in tables:
                name = table['Name']
                location = table['StorageDescriptor'].get('Location', 'N/A')
                columns = len(table['StorageDescriptor'].get('Columns', []))
                print(f"  • {name} ({columns} columnas) - {location}")
        else:
            print("❌ No hay tablas registradas. Ejecuta: python main.py glue")
            
    except Exception as e:
        print(f"❌ Error listando tablas: {e}")

def show_table_schema(table_name: str, config: dict):
    """Mostrar esquema de una tabla"""
    try:
        glue = boto3.client('glue', region_name=config['aws'].get('region', 'us-east-2'))
        database = config['glue']['database_name']
        
        response = glue.get_table(DatabaseName=database, Name=table_name)
        table = response['Table']
        columns = table['StorageDescriptor'].get('Columns', [])
        
        print(f"📊 ESQUEMA DE TABLA: {table_name}")
        print("-" * 50)
        for col in columns:
            name = col['Name']
            data_type = col['Type']
            print(f"  {name:20} | {data_type}")
            
    except Exception as e:
        print(f"❌ Error obteniendo esquema: {e}")

def run_interactive_athena():
    """Ejecutar Athena interactivo"""
    config = load_config()
    database = config['glue']['database_name']
    
    print("🚀 ATHENA INTERACTIVO")
    print("=" * 50)
    print(f"📂 Base de datos: {database}")
    print()
    
    # Mostrar tablas disponibles
    list_available_tables(config)
    print()
    
    print("💡 COMANDOS ESPECIALES:")
    print("  • 'tables' - Listar tablas disponibles")
    print("  • 'schema TABLA' - Ver esquema de una tabla")
    print("  • 'exit' - Salir")
    print("  • Para consultas multilínea: termina con ';' y presiona Enter")
    print()
    
    # Ejemplos de consultas
    print("📝 EJEMPLOS DE CONSULTAS:")
    print("  SELECT * FROM year_2026 LIMIT 5;")
    print("  SELECT COUNT(*) FROM year_2026;")
    print("  SELECT action, COUNT(*) FROM year_2026 GROUP BY action;")
    print()
    
    query_buffer = []
    
    while True:
        try:
            # Prompt diferente si estamos en modo multilínea
            if query_buffer:
                line = input("     ... ").strip()
            else:
                line = input("🔍 SQL> ").strip()
            
            if not line and not query_buffer:
                continue
            
            # Comandos especiales solo funcionan en línea única
            if not query_buffer:
                if line.lower() == 'exit':
                    print("👋 ¡Hasta luego!")
                    break
                    
                elif line.lower() == 'tables':
                    list_available_tables(config)
                    continue
                    
                elif line.lower().startswith('schema '):
                    table_name = line[7:].strip()
                    show_table_schema(table_name, config)
                    continue
            
            # Agregar línea al buffer
            if line:
                query_buffer.append(line)
            
            # Si la línea termina con ';' o es una línea vacía después de tener contenido
            if (line.endswith(';') or (not line and query_buffer)):
                # Unir todas las líneas en una consulta
                full_query = ' '.join(query_buffer).strip()
                
                if full_query:
                    # Ejecutar consulta SQL
                    execute_athena_query(full_query, config)
                
                # Limpiar buffer
                query_buffer = []
                print()
            
        except KeyboardInterrupt:
            print("\n👋 ¡Hasta luego!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            query_buffer = []  # Limpiar buffer en caso de error

if __name__ == "__main__":
    run_interactive_athena()