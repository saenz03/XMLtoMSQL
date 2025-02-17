import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import mysql.connector
from mysql.connector import Error
from preprocesamiento import parse_xml  # type: ignore
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TransformData(beam.DoFn):
    """Clase que toma cada diccionario de datos y lo transforma para Beam"""
    def process(self, element):
        yield {
            "state": element.get("state", ""),
            "city": element.get("city", ""),
            "colony": element.get("colony", ""),
            "street": element.get("street", ""),
            "external_num": element.get("external_num", ""),
            "code": element.get("code", ""),
            "type": element.get("type", ""),
            "purpose": element.get("purpose", ""),
            "price": float(element.get("price", 0)) if element.get("price") else 0.0,
            "mail_contact": element.get("mail_contact", ""),
            "phone_contact": element.get("phone_contact", "")
        }

class InsertDataToMySQL(beam.DoFn):
    def process(self, element):
        connection = None
        try:
            logger.info(f"Procesando datos para la inserción: {element}")

            # Conectar a MySQL
            connection = mysql.connector.connect(
                host="localhost",  # Ajusta esto si estás usando Docker o un host diferente
                user="habi_user",
                password="habi_pass",
                database="habi_db"
            )
            cursor = connection.cursor()

            # Verificar si el usuario ya existe
            cursor.execute("SELECT id FROM usuarios WHERE mail_contact = %s", (element.get("mail_contact"),))
            user_id = cursor.fetchone()
            logger.info(f"ID del usuario encontrado: {user_id}")

            if not user_id:
                # Si el usuario no existe, insertarlo en la tabla 'usuarios'
                cursor.execute("INSERT INTO usuarios (mail_contact) VALUES (%s)", (element.get("mail_contact"),))
                connection.commit()  # Confirmar transacción
                user_id = cursor.lastrowid  # Obtener el id del nuevo usuario
                logger.info(f"Nuevo usuario insertado con ID: {user_id}")
            else:
                user_id = user_id[0]  # Obtener id del usuario existente
                logger.info(f"Usuario existente con ID: {user_id}")

            # Verificar y convertir 'external_num' de 'null' a None
            external_num = element.get("external_num")
            if external_num == 'null':
                external_num = None  # Convertir 'null' string a None

            # Preparar y ejecutar la consulta para insertar en 'propiedades'
            insert_query = """
                INSERT INTO propiedades (state, city, colony, street, external_num, code, type, purpose, price, mail_contact, phone_contact, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data = (
                element.get("state"),
                element.get("city"),
                element.get("colony"),
                element.get("street"),
                external_num,
                element.get("code"),
                element.get("type"),
                element.get("purpose"),
                element.get("price"),
                element.get("mail_contact"),
                element.get("phone_contact"),
                user_id
            )

            logger.info(f"Ejecutando consulta: {insert_query} con datos: {data}")
            cursor.execute(insert_query, data)
            connection.commit()  # Confirmar la inserción
            logger.info(f"Datos insertados con éxito: {element}")

        except mysql.connector.Error as err:
            logger.error(f"Error al procesar los datos en MySQL: {err}")
            if connection:
                connection.rollback()
        except Exception as e:
            logger.error(f"Error inesperado: {e}")
            if connection:
                connection.rollback()
        finally:
            if connection:
                cursor.close()
                connection.close()

        return [element]  # O simplemente `return []` si no necesitas devolver nada

def run():
    try:
        options = PipelineOptions()
        xml_file_path = os.path.join(os.path.dirname(__file__), "feed.xml")
        
        logger.info(f"Iniciando procesamiento de archivo XML: {xml_file_path}")
        
        # Cargar datos del XML
        xml_data = parse_xml(xml_file_path)
        
        if not xml_data:
            logger.error("No se encontraron datos en el XML")
            return
        
        logger.info(f"Se encontraron JU {len(xml_data)} registros en el XML")

        # Procesar primero un conjunto pequeño para prueba
        test_data = xml_data[:10]  # Primeros 10 registros
        logger.info("Procesando primeros 10 registros para prueba")

        with beam.Pipeline(options=options) as p:
            (p 
             | "Crear PCollection" >> beam.Create(test_data)
             | "Transformar Datos" >> beam.ParDo(TransformData())
             | "Insertar en MySQL" >> beam.ParDo(InsertDataToMySQL())
            )
            
        logger.info("Pipeline ejecutado exitosamente")

    except Exception as e:
        logger.error(f"Error en la ejecución del pipeline: {e}")
        raise

if __name__ == "__main__":
    run()
