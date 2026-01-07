"""
Módulo para gestión del AWS Glue Data Catalog
Registra automáticamente tablas y esquemas
"""

import boto3
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class GlueCatalogManager:
    """Gestor del catálogo de datos de AWS Glue"""
    
    def __init__(self, database_name: str = "datalake_db"):
        self.glue = boto3.client('glue')
        self.s3 = boto3.client('s3')
        self.database_name = database_name
        
    def create_database(self) -> bool:
        """Crear base de datos en Glue si no existe"""
        try:
            self.glue.create_database(
                DatabaseInput={
                    'Name': self.database_name,
                    'Description': 'Data Lake database for processed data'
                }
            )
            logger.info(f"Database {self.database_name} created")
            return True
        except self.glue.exceptions.AlreadyExistsException:
            logger.info(f"Database {self.database_name} already exists")
            return True
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return False
    
    def register_parquet_table(self, 
                              table_name: str,
                              s3_location: str,
                              columns: List[Dict],
                              partition_keys: Optional[List[Dict]] = None) -> bool:
        """Registrar tabla Parquet en el catálogo"""
        try:
            # Asegurar que la database existe
            self.create_database()
            
            table_input = {
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': s3_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': 'parquet',
                    'compressionType': 'snappy'
                }
            }
            
            if partition_keys:
                table_input['PartitionKeys'] = partition_keys
            
            self.glue.create_table(
                DatabaseName=self.database_name,
                TableInput=table_input
            )
            
            logger.info(f"Table {table_name} registered in Glue Catalog")
            return True
            
        except self.glue.exceptions.AlreadyExistsException:
            logger.info(f"Table {table_name} already exists")
            return True
        except Exception as e:
            logger.error(f"Error registering table {table_name}: {e}")
            return False
    
    def auto_register_from_parquet(self, bucket: str, key: str, table_name: str) -> bool:
        """Auto-registrar tabla basada en archivo Parquet existente"""
        try:
            # Leer muestra del archivo para inferir esquema
            s3_path = f"s3://{bucket}/{key}"
            df_sample = pd.read_parquet(s3_path, nrows=1)
            
            # Convertir tipos de pandas a Glue
            columns = []
            for col_name, dtype in df_sample.dtypes.items():
                glue_type = self._pandas_to_glue_type(dtype)
                columns.append({
                    'Name': col_name,
                    'Type': glue_type
                })
            
            # Ubicación de la tabla (directorio padre)
            s3_location = f"s3://{bucket}/{'/'.join(key.split('/')[:-1])}/"
            
            # Particiones por año/mes/día si existe la estructura
            partition_keys = []
            if '/year=' in key:
                partition_keys = [
                    {'Name': 'year', 'Type': 'string'},
                    {'Name': 'month', 'Type': 'string'},
                    {'Name': 'day', 'Type': 'string'}
                ]
            
            return self.register_parquet_table(
                table_name=table_name,
                s3_location=s3_location,
                columns=columns,
                partition_keys=partition_keys if partition_keys else None
            )
            
        except Exception as e:
            logger.error(f"Error auto-registering table: {e}")
            return False
    
    def _pandas_to_glue_type(self, pandas_dtype) -> str:
        """Convertir tipos de pandas a tipos de Glue"""
        dtype_str = str(pandas_dtype)
        
        if 'int' in dtype_str:
            return 'bigint'
        elif 'float' in dtype_str:
            return 'double'
        elif 'bool' in dtype_str:
            return 'boolean'
        elif 'datetime' in dtype_str:
            return 'timestamp'
        else:
            return 'string'
    
    def list_tables(self) -> List[str]:
        """Listar todas las tablas en la database"""
        try:
            response = self.glue.get_tables(DatabaseName=self.database_name)
            return [table['Name'] for table in response['TableList']]
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """Obtener información de una tabla"""
        try:
            response = self.glue.get_table(
                DatabaseName=self.database_name,
                Name=table_name
            )
            return response['Table']
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return None