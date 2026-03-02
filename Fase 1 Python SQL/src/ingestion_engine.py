import os
import logging
import psycopg2
import random
from datetime import datetime
from dotenv import load_dotenv

# --- CONFIGURACIÓN DE RUTAS DINÁMICAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Define las rutas subiendo un nivel hacia la raíz del proyecto
LOGS_PATH = os.path.join(BASE_DIR, "..", "logs", "pipeline.log")
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")

# 1. Configuración de Logs con ruta absoluta
logging.basicConfig(
    filename=LOGS_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 2. Carga de variables de entorno con ruta absoluta
load_dotenv(dotenv_path=ENV_PATH)
# Mantiene tus contraseñas fuera del código
load_dotenv(dotenv_path='../.env')

def validar_datos(servicio, monto):
    """Módulo de Data Quality: Evita basura en la base de datos."""
    if monto <= 0:
        return False
    if servicio not in ['EC2', 'S3', 'RDS', 'Lambda', 'Redshift', 'DynamoDB']:
        return False
    return True

def generar_datos_aws(n=10000):
    """Genera 10,000 registros simulando logs reales de AWS."""
    servicios = ['EC2', 'S3', 'RDS', 'Lambda', 'Redshift', 'DynamoDB']
    regiones = ['us-east-1', 'us-west-2', 'sa-east-1', 'eu-west-1']
    
    registros_validos = []
    for _ in range(n):
        srv = random.choice(servicios)
        val = round(random.uniform(0.01, 800.0), 2)
        reg = random.choice(regiones)
        
        if validar_datos(srv, val):
            registros_validos.append((srv, val, reg))
            
    return registros_validos

def ejecutar_pipeline():
    conn = None
    try:
        logging.info("Iniciando Pipeline de Ingesta Masiva...")
        
        # Conexión usando las variables de tu archivo .env
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            port=os.getenv('DB_PORT')
        )
        cur = conn.cursor()

        # 3. PREPARACIÓN DEL ESQUEMA (Idempotencia)
        # Garantizamos que el esquema y la extensión UUID existan
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw_aws_billing;")
        cur.execute("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\";")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_aws_billing.facturacion (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                servicio VARCHAR(50),
                monto DECIMAL(12,2),
                region VARCHAR(30),
                fecha_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 4. CARGA MASIVA (BATCH INSERT)
        # Es mucho más rápido que hacer 10,000 inserts individuales
        datos = generar_datos_aws(10000)
        query_insert = "INSERT INTO raw_aws_billing.facturacion (servicio, monto, region) VALUES (%s, %s, %s)"
        
        cur.executemany(query_insert, datos)
        
        conn.commit()
        logging.info(f"Éxito: Se cargaron {len(datos)} registros en raw_aws_billing.facturacion.")
        print(f"Pipeline terminado. Se procesaron {len(datos)} filas.")

    except Exception as e:
        logging.error(f"Error crítico en el pipeline: {e}")
        print(f"Error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            logging.info("🔌 Conexión a PostgreSQL cerrada de forma segura.")

if __name__ == "__main__":
    ejecutar_pipeline()