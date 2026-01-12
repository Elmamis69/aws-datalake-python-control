"""
Utilidades comunes para el dashboard
"""

import psutil
import os
from datetime import datetime
from pathlib import Path

def format_size(size_bytes):
    """Formatear bytes a formato legible"""
    if size_bytes == 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"

def check_worker_status():
    """Verificar si el worker está corriendo"""
    try:
        current_dir = str(Path(__file__).parent.parent.parent)
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
            try:
                if proc.info['name'] and 'python' in proc.info['name'].lower():
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    
                    if ('main.py worker' in cmdline or 
                        (current_dir in cmdline and 'worker' in cmdline)):
                        
                        create_time = datetime.fromtimestamp(proc.info['create_time'])
                        uptime = datetime.now() - create_time
                        
                        return {
                            'running': True,
                            'pid': proc.info['pid'],
                            'since': f"Activo {uptime.seconds//60}m"
                        }
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        
        return {'running': False, 'pid': None, 'since': 'Detenido'}
        
    except ImportError:
        return {'running': None, 'pid': None, 'since': 'psutil no instalado'}
    except Exception as e:
        return {'running': None, 'pid': None, 'since': f'Error: {str(e)[:20]}...'}