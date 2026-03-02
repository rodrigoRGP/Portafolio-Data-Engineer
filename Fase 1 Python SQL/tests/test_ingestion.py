import sys
import os

# Añadimos la carpeta src al camino de búsqueda de Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from ingestion_engine import validar_datos

def test_monto_positivo_es_valido():
    """Prueba que un monto normal sea aceptado."""
    assert validar_datos('EC2', 150.0) is True

def test_monto_negativo_es_invalido():
    """Prueba que el sistema rechace montos negativos (Regla de negocio)."""
    assert validar_datos('S3', -10.0) is False

def test_servicio_no_autorizado_es_invalido():
    """Prueba que no se permitan servicios fuera del catálogo de AWS."""
    assert validar_datos('Netflix_Subscription', 20.0) is False

def test_monto_cero_es_invalido():
    """Prueba el límite: el gasto cero no debería procesarse."""
    assert validar_datos('Lambda', 0.0) is False