# ☁️ AWS Billing Data Pipeline
Sistema de ingesta y analítica de grado empresarial para el monitoreo de costos de AWS.

## Resumen del Proyecto
Este pipeline automatiza la extracción, validación y carga de +10,000 registros de facturación de AWS en un entorno PostgreSQL contenedorizado. Diseñado bajo principios de **Clean Code** y **Observabilidad**.

## Stack Tecnológico
- **Lenguaje:** Python 3.13
- **Base de Datos:** PostgreSQL en Docker (Agnóstico vía DBeaver)
- **Calidad:** Pytest para Unit Testing.
- **Seguridad:** Manejo de secretos mediante variables de entorno (.env).

## Arquitectura de la Solución
1. **Ingestión:** Script de Python con lógica de `Data Quality` (rechazo de montos negativos y servicios inválidos).
2. **Almacenamiento:** Esquema relacional en Postgres con uso de UUIDs para escalabilidad.
3. **Analítica:** Consultas avanzadas utilizando CTEs y Window Functions (RANK) para reportes regionales.

## Cómo ejecutar
1. Levantar Docker: `docker start mi-postgres`
2. Instalar dependencias: `pip install -r requirements.txt`
3. Ejecutar Pipeline: `python3 src/ingestion_engine.py`
4. Correr Tests: `pytest tests/`