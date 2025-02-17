import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import mysql.connector
from mysql.connector import Error
import logging
from typing import Dict, Any, List

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TransformData(beam.DoFn):
    def process(self, element: Dict[str, Any]) -> List[Dict[str, Any]]: # type: ignore
        try:
            transformed = {
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
            yield transformed
        except Exception as e:
            logger.error(f"Error en transformación: {e}")
            raise

class InsertDataToMySQL(beam.DoFn):
    def __init__(self):
        self.connection = None
        self.cursor = None

    def start_bundle(self):
        try:
            logger.info("Iniciando conexión a MySQL...")
            self.connection = mysql.connector.connect(
                host="localhost",
                user="habi_user",
                password="habi_pass",
                database="habi_db",
                port=3306
            )
            self.cursor = self.connection.cursor(buffered=True)
            logger.info("Conexión a MySQL establecida exitosamente")
        except Error as e:
            logger.error(f"Error de conexión: {e}")
            raise

    def process(self, element: Dict[str, Any]):
        try:
            if not self.connection or not self.connection.is_connected():
                logger.warning("Reconectando a MySQL...")
                self.start_bundle()

            # Primero, insertar o obtener usuario
            self.cursor.execute(
                "SELECT id FROM usuarios WHERE mail_contact = %s",
                (element["mail_contact"],)
            )
            user = self.cursor.fetchone()

            if not user:
                logger.info(f"Insertando nuevo usuario con mail: {element['mail_contact']}")
                self.cursor.execute(
                    "INSERT INTO usuarios (mail_contact) VALUES (%s)",
                    (element["mail_contact"],)
                )
                self.connection.commit()
                user_id = self.cursor.lastrowid
                logger.info(f"Nuevo usuario creado con ID: {user_id}")
            else:
                user_id = user[0]
                logger.info(f"Usuario existente encontrado con ID: {user_id}")

            # Luego, insertar propiedad
            logger.info(f"Insertando propiedad para usuario_id: {user_id}")
            property_data = (
                element["state"],
                element["city"],
                element["colony"],
                element["street"],
                element["external_num"],
                element["code"],
                element["type"],
                element["purpose"],
                element["price"],
                element["mail_contact"],
                element["phone_contact"],
                user_id
            )

            self.cursor.execute("""
                INSERT INTO propiedades 
                (state, city, colony, street, external_num, code, type,
                purpose, price, mail_contact, phone_contact, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, property_data)
            
            self.connection.commit()
            logger.info(f"Propiedad insertada exitosamente para usuario_id: {user_id}")
            
            yield element

        except Error as e:
            logger.error(f"Error en proceso de inserción: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    def finish_bundle(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            logger.info("Conexión cerrada")

def run():
    try:
        options = PipelineOptions()
        xml_file_path = os.path.join(os.path.dirname(__file__), "feed.xml")
        
        logger.info(f"Iniciando procesamiento de archivo XML: {xml_file_path}")
        
        from preprocesamiento import parse_xml # type: ignore
        xml_data = parse_xml(xml_file_path)
        
        if not xml_data:
            logger.error("No se encontraron datos en el XML")
            return
            
        logger.info(f"Se encontraron {len(xml_data)} registros en el XML")

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