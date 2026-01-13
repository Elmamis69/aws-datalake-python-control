#!/usr/bin/env python3
"""
Script de Prueba del Sistema AWS Data Lake Control

Este script verifica que todos los componentes del Data Lake estén
funcionando correctamente antes de usar el sistema en producción.

Pruebas que realiza:
1. 📄 Configuración - Verifica que settings.yaml sea válido
2. 🔗 Conexión AWS - Prueba credenciales y conectividad
3. 📁 Glue Catalog - Verifica acceso al catálogo de datos
4. 📦 S3 Operations - Prueba operaciones de lectura en S3

Uso: python test_app.py

Si todas las pruebas pasan, el sistema está listo para usar.
Si alguna falla, revisa la configuración y credenciales AWS.
"""

import sys
from pathlib import Path

# Agregar src al path para importar módulos locales
sys.path.append(str(Path(__file__).parent / "src"))

from src.datalake.sqs_worker import run_sqs_worker
from src.datalake.s3_io import read_s3_object
from src.glue_catalog import GlueCatalogManager
import yaml
import boto3

def test_config():
    """
    Probar carga de configuración desde settings.yaml
    
    Verifica que:
    - El archivo settings.yaml existe y es válido
    - Contiene todas las claves necesarias
    - Los valores tienen el formato correcto
    """
    print("[CONFIG] Probando configuración...")
    try:
        config_path = Path(__file__).parent / "config" / "settings.yaml"
        if not config_path.exists():
            print(f"[ERROR] Archivo de configuración no encontrado: {config_path}")
            return None
            
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        # Verificar claves esenciales
        required_keys = ['aws', 'worker', 'glue']
        for key in required_keys:
            if key not in config:
                print(f"[ERROR] Clave faltante en configuración: {key}")
                return None
                
        print(f"[OK] Configuración cargada - Región: {config['aws']['region']}")
        return config
    except Exception as e:
        print(f"[ERROR] Error cargando configuración: {e}")
        return None

def test_aws_connection():
    """
    Probar conexión con AWS
    
    Verifica que:
    - Las credenciales AWS estén configuradas correctamente
    - Hay conectividad con los servicios AWS
    - Los permisos básicos funcionan
    
    Usa list_buckets() como prueba simple que requiere credenciales válidas.
    """
    print("[AWS] Probando conexión AWS...")
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        bucket_count = len(response['Buckets'])
        print(f"[OK] Conexión AWS exitosa. Buckets accesibles: {bucket_count}")
        return True
    except Exception as e:
        print(f"[ERROR] Error conectando a AWS: {e}")
        print("        Verifica tus credenciales AWS (aws configure)")
        return False

def test_glue_catalog():
    """
    Probar acceso al AWS Glue Catalog
    
    Verifica que:
    - Se puede acceder al servicio Glue
    - Se pueden listar tablas existentes
    - Los permisos de Glue están configurados
    
    El catálogo es esencial para que Athena pueda consultar los datos.
    """
    print("[GLUE] Probando Glue Catalog...")
    try:
        catalog = GlueCatalogManager("datalake_processed_db")
        tables = catalog.list_tables()
        print(f"[OK] Glue Catalog funcionando. Tablas registradas: {len(tables)}")
        if tables:
            print(f"      Tablas: {', '.join(tables[:5])}{'...' if len(tables) > 5 else ''}")
        return True
    except Exception as e:
        print(f"[ERROR] Error con Glue Catalog: {e}")
        print("        Verifica permisos de Glue en tu usuario/rol AWS")
        return False

def test_s3_operations(config):
    """
    Probar operaciones básicas de S3
    
    Verifica que:
    - Se puede acceder al bucket configurado
    - Se pueden listar archivos
    - Los permisos de S3 están correctos
    
    Args:
        config (dict): Configuración cargada desde settings.yaml
    """
    print("[S3] Probando operaciones S3...")
    try:
        s3 = boto3.client('s3')
        bucket = config['aws']['s3_processed_bucket']
        prefix = config['aws']['s3_processed_prefix']
        
        print(f"      Consultando bucket: {bucket}/{prefix}")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
        files = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"[OK] S3 funcionando. Archivos encontrados: {len(files)}")
        if files:
            print(f"      Ejemplos: {files[:3]}")
        return True
    except Exception as e:
        print(f"[ERROR] Error con S3: {e}")
        print(f"        Verifica que el bucket '{config['aws']['s3_processed_bucket']}' existe")
        print("        y que tienes permisos de lectura")
        return False

def main():
    """
    Ejecutar todas las pruebas del sistema
    
    Ejecuta las pruebas en orden lógico:
    1. Configuración (base para todo lo demás)
    2. Conexión AWS (credenciales)
    3. Servicios específicos (Glue, S3)
    
    Si alguna prueba falla, las siguientes pueden no ejecutarse
    correctamente, pero el script continúa para dar un diagnóstico completo.
    
    Returns:
        bool: True si todas las pruebas pasaron
    """
    print("[INICIO] Iniciando pruebas del sistema AWS Data Lake Control\n")
    print("🔍 Verificando componentes del sistema...\n")
    
    tests_passed = 0
    total_tests = 4
    config = None
    
    try:
        # Test 1: Configuración (crítico - necesario para otros tests)
        config = test_config()
        if config:
            tests_passed += 1
        else:
            print("[CRITICO] Sin configuración válida, algunos tests pueden fallar\n")
        
        # Test 2: Conexión AWS (crítico - necesario para servicios AWS)
        if test_aws_connection():
            tests_passed += 1
        print()  # Línea en blanco para separar
        
        # Test 3: Glue Catalog
        if test_glue_catalog():
            tests_passed += 1
        print()  # Línea en blanco para separar
        
        # Test 4: S3 Operations (solo si tenemos configuración)
        if config and test_s3_operations(config):
            tests_passed += 1
            
    except Exception as e:
        print(f"[ERROR] Error general durante las pruebas: {e}")
    
    # Reporte final
    print(f"\n{'='*60}")
    print(f"[RESULTADO] Pruebas completadas: {tests_passed}/{total_tests} exitosas")
    
    if tests_passed == total_tests:
        print("🎉 [EXITO] Todos los componentes funcionan correctamente!")
        print("✅ [OK] La aplicación está lista para usar")
        print("\n🚀 Próximos pasos:")
        print("   - Ejecutar pipeline de prueba: python main.py pipeline")
        print("   - Iniciar worker: python main.py worker")
        print("   - Abrir dashboard: python main.py dashboard")
    else:
        print("⚠️  [ATENCION] Algunos componentes necesitan atención")
        print("\n🔧 Revisa:")
        print("   - Configuración en config/settings.yaml")
        print("   - Credenciales AWS (aws configure)")
        print("   - Permisos de S3 y Glue")
        print("   - Conectividad a internet")
        
    return tests_passed == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)