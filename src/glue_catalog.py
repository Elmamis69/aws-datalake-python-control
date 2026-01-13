"""
AWS Glue Data Catalog Manager

Este módulo gestiona el catálogo de datos de AWS Glue, que actúa como
un "diccionario de datos" centralizado para el Data Lake.

Funcionalidades principales:
1. Crear y gestionar bases de datos en Glue
2. Registrar tablas automáticamente desde archivos Parquet
3. Inferir esquemas de datos automáticamente
4. Manejar particiones para optimizar consultas
5. Proporcionar metadatos para Athena y otras herramientas

¿Por qué es importante el catálogo?
- Permite consultar datos con SQL en Athena
- Proporciona esquemas consistentes
- Optimiza consultas con particiones
- Facilita el descubrimiento de datos
- Integra con herramientas de BI

Flujo típico:
1. Se procesa un archivo JSONL → Parquet
2. Se registra automáticamente en el catálogo
3. Los datos quedan disponibles para consultas SQL

Uso:
    catalog = GlueCatalogManager("mi_database")
    catalog.auto_register_from_parquet("bucket", "datos/archivo.parquet", "mi_tabla")
"""

import boto3
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class GlueCatalogManager:
    """
    Gestor del catálogo de datos de AWS Glue
    
    Esta clase encapsula todas las operaciones necesarias para mantener
    el catálogo de datos actualizado automáticamente.
    
    Características:
    - Creación automática de bases de datos
    - Registro de tablas con inferencia de esquemas
    - Soporte para particiones (year/month/day)
    - Conversión automática de tipos de datos
    - Manejo de errores y logging detallado
    """
    
    def __init__(self, database_name: str = "datalake_db"):
        """
        Inicializar el gestor del catálogo
        
        Args:
            database_name (str): Nombre de la base de datos en Glue
        """
        self.glue = boto3.client('glue')
        self.s3 = boto3.client('s3')
        self.database_name = database_name
        logger.info(f"📁 Inicializando GlueCatalogManager para database: {database_name}")
        
    def create_database(self) -> bool:
        """
        Crear base de datos en Glue si no existe
        
        La base de datos actúa como un namespace para agrupar tablas relacionadas.
        Es necesaria antes de registrar cualquier tabla.
        
        Returns:
            bool: True si se creó o ya existía, False si hubo error
        """
        try:
            self.glue.create_database(
                DatabaseInput={
                    'Name': self.database_name,
                    'Description': f'Data Lake database for processed data - Created {datetime.now().isoformat()}'
                }
            )
            logger.info(f"✅ Database {self.database_name} creada exitosamente")
            return True
        except self.glue.exceptions.AlreadyExistsException:
            logger.info(f"📁 Database {self.database_name} ya existe")
            return True
        except Exception as e:
            logger.error(f"❌ Error creando database: {e}")
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
        """
        Auto-registrar tabla basada en archivo Parquet existente
        
        Esta es la función más importante del módulo. Automáticamente:
        1. Lee una muestra del archivo Parquet
        2. Infiere el esquema (nombres y tipos de columnas)
        3. Detecta si hay particiones (year/month/day)
        4. Registra la tabla en el catálogo
        
        Después de esto, los datos estarán disponibles para consultas SQL en Athena.
        
        Args:
            bucket (str): Nombre del bucket S3
            key (str): Ruta completa del archivo Parquet
            table_name (str): Nombre que tendrá la tabla en el catálogo
        
        Returns:
            bool: True si se registró exitosamente
            
        Example:
            # Archivo: s3://mi-bucket/procesados/eventos/2024/01/15/datos.parquet
            catalog.auto_register_from_parquet(
                "mi-bucket", 
                "procesados/eventos/2024/01/15/datos.parquet", 
                "eventos"
            )
            # Resultado: Tabla 'eventos' disponible en Athena con particiones por fecha
        """
        try:
            logger.info(f"🔍 Auto-registrando tabla {table_name} desde s3://{bucket}/{key}")
            
            # Leer muestra del archivo para inferir esquema
            s3_path = f"s3://{bucket}/{key}"
            logger.info(f"   Leyendo muestra para inferir esquema...")
            df_sample = pd.read_parquet(s3_path, nrows=1)
            
            # Convertir tipos de pandas a tipos compatibles con Glue/Athena
            columns = []
            for col_name, dtype in df_sample.dtypes.items():
                glue_type = self._pandas_to_glue_type(dtype)
                columns.append({
                    'Name': col_name,
                    'Type': glue_type
                })
                logger.info(f"   Columna: {col_name} ({dtype} → {glue_type})")
            
            # Determinar ubicación de la tabla (directorio padre del archivo)
            # Ejemplo: procesados/eventos/2024/01/15/datos.parquet → s3://bucket/procesados/eventos/
            key_parts = key.split('/')
            if len(key_parts) > 1:
                table_location_parts = key_parts[:-1]  # Remover nombre del archivo
                # Si hay particiones, remover las carpetas de partición
                if any('=' in part for part in key_parts):
                    # Encontrar donde empiezan las particiones
                    partition_start = next(i for i, part in enumerate(key_parts) if '=' in part)
                    table_location_parts = key_parts[:partition_start]
                s3_location = f"s3://{bucket}/{'/'.join(table_location_parts)}/"
            else:
                s3_location = f"s3://{bucket}/"
            
            logger.info(f"   Ubicación de tabla: {s3_location}")
            
            # Detectar particiones automáticamente
            partition_keys = []
            if '/year=' in key:
                partition_keys = [
                    {'Name': 'year', 'Type': 'string'},
                    {'Name': 'month', 'Type': 'string'},
                    {'Name': 'day', 'Type': 'string'}
                ]
                logger.info(f"   Particiones detectadas: year/month/day")
            
            # Registrar la tabla
            return self.register_parquet_table(
                table_name=table_name,
                s3_location=s3_location,
                columns=columns,
                partition_keys=partition_keys if partition_keys else None
            )
            
        except Exception as e:
            logger.error(f"❌ Error auto-registrando tabla {table_name}: {e}")
            return False
    
    def _pandas_to_glue_type(self, pandas_dtype) -> str:
        """
        Convertir tipos de datos de pandas a tipos compatibles con Glue/Athena
        
        Esta conversión es crucial para que las consultas SQL funcionen correctamente.
        Glue/Athena usa tipos de datos similares a Hive.
        
        Mapeo de tipos:
        - int* → bigint (enteros de cualquier tamaño)
        - float* → double (números decimales)
        - bool → boolean
        - datetime* → timestamp
        - object/string → string (texto)
        
        Args:
            pandas_dtype: Tipo de dato de pandas (ej: int64, float64, object)
            
        Returns:
            str: Tipo de dato compatible con Glue (ej: 'bigint', 'string')
        """
        dtype_str = str(pandas_dtype)
        
        if 'int' in dtype_str:
            return 'bigint'      # Enteros (int8, int16, int32, int64)
        elif 'float' in dtype_str:
            return 'double'      # Decimales (float32, float64)
        elif 'bool' in dtype_str:
            return 'boolean'     # Booleanos
        elif 'datetime' in dtype_str:
            return 'timestamp'   # Fechas y horas
        else:
            return 'string'      # Todo lo demás (object, string, etc.)
    
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