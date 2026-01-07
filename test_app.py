#!/usr/bin/env python3
"""
Script de prueba para verificar que todos los componentes funcionan
"""

import sys
from pathlib import Path

# Agregar src al path
sys.path.append(str(Path(__file__).parent / "src"))

from src.datalake.sqs_worker import run_sqs_worker
from src.datalake.s3_io import read_s3_object
from src.glue_catalog import GlueCatalogManager
import yaml
import boto3

def test_config():
    """Probar carga de configuración"""
    print("[CONFIG] Probando configuración...")
    config_path = Path(__file__).parent / "config" / "settings.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    print(f"[OK] Configuración cargada: {config['aws']['region']}")
    return config

def test_aws_connection():
    """Probar conexión con AWS"""
    print("[AWS] Probando conexión AWS...")
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        print(f"[OK] Conexión AWS exitosa. Buckets encontrados: {len(response['Buckets'])}")
        return True
    except Exception as e:
        print(f"[ERROR] Error conectando a AWS: {e}")
        return False

def test_glue_catalog():
    """Probar Glue Catalog"""
    print("[GLUE] Probando Glue Catalog...")
    try:
        catalog = GlueCatalogManager("datalake_processed_db")
        tables = catalog.list_tables()
        print(f"[OK] Glue Catalog funcionando. Tablas: {tables}")
        return True
    except Exception as e:
        print(f"[ERROR] Error con Glue Catalog: {e}")
        return False

def test_s3_operations(config):
    """Probar operaciones S3"""
    print("[S3] Probando operaciones S3...")
    try:
        s3 = boto3.client('s3')
        bucket = config['aws']['s3_processed_bucket']
        prefix = config['aws']['s3_processed_prefix']
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
        files = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"[OK] S3 funcionando. Archivos encontrados: {len(files)}")
        return True
    except Exception as e:
        print(f"[ERROR] Error con S3: {e}")
        return False

def main():
    """Ejecutar todas las pruebas"""
    print("[INICIO] Iniciando pruebas del sistema AWS Data Lake Control\n")
    
    tests_passed = 0
    total_tests = 4
    
    try:
        # Test 1: Configuración
        config = test_config()
        tests_passed += 1
        
        # Test 2: Conexión AWS
        if test_aws_connection():
            tests_passed += 1
        
        # Test 3: Glue Catalog
        if test_glue_catalog():
            tests_passed += 1
        
        # Test 4: S3 Operations
        if test_s3_operations(config):
            tests_passed += 1
            
    except Exception as e:
        print(f"[ERROR] Error general: {e}")
    
    print(f"\n[RESULTADO] Pruebas: {tests_passed}/{total_tests} exitosas")
    
    if tests_passed == total_tests:
        print("[EXITO] Todos los componentes funcionan correctamente!")
        print("[OK] La aplicación está lista para usar")
    else:
        print("[ATENCION] Algunos componentes necesitan atención")
        
    return tests_passed == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)