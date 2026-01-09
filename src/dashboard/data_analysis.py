"""
Módulo de análisis de datos para el dashboard
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def analyze_dataframe(df, file_info=None):
    """Análisis completo de DataFrame"""
    analysis = {
        'basic_info': get_basic_info(df, file_info),
        'column_analysis': analyze_columns(df),
        'data_quality': assess_data_quality(df),
        'patterns': detect_patterns(df)
    }
    return analysis

def get_basic_info(df, file_info=None):
    """Información básica del DataFrame"""
    memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
    
    info = {
        'rows': df.shape[0],
        'columns': df.shape[1],
        'memory_mb': round(memory_mb, 2),
        'column_names': list(df.columns),
        'dtypes': df.dtypes.to_dict()
    }
    
    if file_info:
        info['file_name'] = file_info.get('name', 'Unknown')
        info['file_path'] = file_info.get('path', 'Unknown')
        info['file_size_mb'] = round(file_info.get('size', 0) / (1024 * 1024), 2)
    
    return info

def analyze_columns(df):
    """Análisis detallado por columna"""
    column_stats = {}
    
    for col in df.columns:
        stats = {
            'dtype': str(df[col].dtype),
            'non_null_count': int(df[col].count()),
            'null_count': int(df[col].isnull().sum()),
            'null_percentage': round((df[col].isnull().sum() / len(df)) * 100, 2),
            'unique_count': int(df[col].nunique()),
            'unique_percentage': round((df[col].nunique() / len(df)) * 100, 2)
        }
        
        # Estadísticas específicas por tipo
        if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
            stats.update({
                'min': df[col].min(),
                'max': df[col].max(),
                'mean': round(df[col].mean(), 2),
                'median': df[col].median(),
                'std': round(df[col].std(), 2)
            })
        elif df[col].dtype == 'object':
            # Top valores para columnas de texto
            top_values = df[col].value_counts().head(5).to_dict()
            stats['top_values'] = top_values
            
            # Longitud promedio para strings
            if df[col].dtype == 'object':
                avg_length = df[col].astype(str).str.len().mean()
                stats['avg_string_length'] = round(avg_length, 1)
        
        column_stats[col] = stats
    
    return column_stats

def assess_data_quality(df):
    """Evaluación de calidad de datos"""
    quality_issues = []
    
    # Duplicados
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        quality_issues.append({
            'type': 'duplicates',
            'count': int(duplicate_count),
            'percentage': round((duplicate_count / len(df)) * 100, 2)
        })
    
    # Columnas completamente vacías
    empty_columns = df.columns[df.isnull().all()].tolist()
    if empty_columns:
        quality_issues.append({
            'type': 'empty_columns',
            'columns': empty_columns,
            'count': len(empty_columns)
        })
    
    # Columnas con alta cardinalidad (posibles IDs)
    high_cardinality = []
    for col in df.columns:
        if df[col].nunique() / len(df) > 0.95 and len(df) > 100:
            high_cardinality.append(col)
    
    if high_cardinality:
        quality_issues.append({
            'type': 'high_cardinality',
            'columns': high_cardinality,
            'count': len(high_cardinality)
        })
    
    return {
        'total_issues': len(quality_issues),
        'issues': quality_issues,
        'overall_score': calculate_quality_score(df, quality_issues)
    }

def detect_patterns(df):
    """Detectar patrones en los datos"""
    patterns = {}
    
    # Detectar columnas de fecha/tiempo
    date_columns = []
    for col in df.columns:
        if df[col].dtype == 'object':
            # Intentar parsear como fecha una muestra
            sample = df[col].dropna().head(100)
            date_like_count = 0
            for val in sample:
                try:
                    pd.to_datetime(val)
                    date_like_count += 1
                except:
                    continue
            
            if date_like_count / len(sample) > 0.8:
                date_columns.append(col)
    
    patterns['date_columns'] = date_columns
    
    # Detectar columnas categóricas (baja cardinalidad)
    categorical_columns = []
    for col in df.columns:
        unique_ratio = df[col].nunique() / len(df)
        if unique_ratio < 0.05 and df[col].nunique() > 1:
            categorical_columns.append({
                'column': col,
                'unique_count': int(df[col].nunique()),
                'unique_ratio': round(unique_ratio, 4)
            })
    
    patterns['categorical_columns'] = categorical_columns
    
    # Detectar posibles columnas de ID
    id_columns = []
    for col in df.columns:
        if (df[col].nunique() == len(df) and 
            len(df) > 10 and 
            col.lower() in ['id', 'uuid', 'key'] or 
            'id' in col.lower()):
            id_columns.append(col)
    
    patterns['id_columns'] = id_columns
    
    return patterns

def calculate_quality_score(df, quality_issues):
    """Calcular score de calidad (0-100)"""
    base_score = 100
    
    # Penalizar por issues
    for issue in quality_issues:
        if issue['type'] == 'duplicates':
            base_score -= min(issue['percentage'], 20)
        elif issue['type'] == 'empty_columns':
            base_score -= issue['count'] * 5
        elif issue['type'] == 'high_cardinality':
            base_score -= issue['count'] * 2
    
    # Penalizar por valores nulos globales
    null_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
    base_score -= min(null_percentage, 30)
    
    return max(0, round(base_score, 1))

def generate_summary_report(analysis):
    """Generar reporte resumen del análisis"""
    basic = analysis['basic_info']
    quality = analysis['data_quality']
    patterns = analysis['patterns']
    
    report = f"""
📊 REPORTE DE ANÁLISIS DE DATOS
{'='*50}

📁 Archivo: {basic.get('file_name', 'N/A')}
📏 Dimensiones: {basic['rows']:,} filas × {basic['columns']} columnas
💾 Memoria: {basic['memory_mb']} MB
⭐ Calidad: {quality['overall_score']}/100

🔍 PATRONES DETECTADOS:
• Columnas de fecha: {len(patterns['date_columns'])}
• Columnas categóricas: {len(patterns['categorical_columns'])}
• Columnas ID: {len(patterns['id_columns'])}

⚠️  ISSUES DE CALIDAD: {quality['total_issues']}
"""
    
    for issue in quality['issues']:
        if issue['type'] == 'duplicates':
            report += f"• Duplicados: {issue['count']} ({issue['percentage']}%)\n"
        elif issue['type'] == 'empty_columns':
            report += f"• Columnas vacías: {issue['count']}\n"
        elif issue['type'] == 'high_cardinality':
            report += f"• Alta cardinalidad: {issue['count']} columnas\n"
    
    return report