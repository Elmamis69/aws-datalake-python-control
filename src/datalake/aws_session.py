"""
aws_session.py
Módulo para crear una sesión boto3 segura y flexible usando perfil, SSO o variables de entorno.
"""
import boto3
from botocore.exceptions import ProfileNotFound
from typing import Optional

def get_boto3_session(profile: Optional[str] = None, region: Optional[str] = None) -> boto3.Session:
    """
    Crea una sesión boto3 usando perfil, variables de entorno o configuración por defecto.
    Args:
        profile (str, optional): Nombre del perfil AWS CLI. Si es None, usa el perfil por defecto o variables de entorno.
        region (str, optional): Región AWS. Si es None, usa la región por defecto.
    Returns:
        boto3.Session: Sesión boto3 configurada.
    """
    try:
        if profile:
            session = boto3.Session(profile_name=profile, region_name=region)
        else:
            session = boto3.Session(region_name=region)
    except ProfileNotFound:
        raise RuntimeError(f"Perfil AWS '{profile}' no encontrado. Verifica tu configuración de AWS CLI.")
    return session
