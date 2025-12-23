# aws-datalake-python-control

Repositorio para un pipeline de ingesta y procesamiento de datos en AWS controlado 100% desde Python, sin Lambda ni Databricks para procesamiento.

## Estructura

- `config/` — Configuración local y de ejemplo
- `src/datalake/` — Código fuente principal
- `scripts/` — Scripts de entrada y utilidades

## Objetivo

Automatizar la ingesta de archivos JSONL en S3, procesarlos externamente y escribir resultados optimizados en Parquet, todo orquestado desde Python.

## Principios

- Event-driven
- Versionado y reproducible
- Sin credenciales hardcodeadas
- AWS solo como backend

## Arquitectura

S3 (RAW JSONL) → SQS → Python Worker → S3 (Parquet)

## Notas

- Asume que la infraestructura AWS (S3, SQS) ya existe
- El worker se ejecuta externamente (no Lambda)
- El código es orientado a producción y extensible

