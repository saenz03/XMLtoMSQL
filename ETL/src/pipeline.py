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
    """Clase que inserta los datos procesados en MySQL"""
    def process(self, element):
        print("Processing:", element) 
        connection = None
        try:
            # Conexión con MySQL
            connection = mysql.connector.connect(
                host='localhost',  # Nombre del servicio en Docker
                port=3306,
                database='habi_db',  # Nombre de la base de datos
                user='habi_user',  # Usuario
                password='habi_pass',  # Contraseña
                use_pure=True
            )


            cursor = connection.cursor()

            # Insertar usuario si no existe
            user_email = element.get("mail_contact")
            cursor.execute("SELECT id FROM usuarios WHERE mail_contact = %s", (user_email,))
            user_id = cursor.fetchone()

            if not user_id:
                cursor.execute("INSERT INTO usuarios (mail_contact) VALUES (%s)", (user_email,))
                connection.commit()
                user_id = cursor.lastrowid
            else:
                user_id = user_id[0]

            # Insertar propiedad
            cursor.execute("""
                INSERT INTO propiedades (state, city, colony, street, external_num, code, type, purpose, price, mail_contact, phone_contact, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                element.get("state"),
                element.get("city"),
                element.get("colony"),
                element.get("street"),
                element.get("external_num"),
                element.get("code"),
                element.get("type"),
                element.get("purpose"),
                element.get("price"),
                element.get("mail_contact"),
                element.get("phone_contact"),
                user_id
            ))
            connection.commit()

        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

        return [element]  # O simplemente `return []` si no necesitas devolver nada





def run():
    try:
        options = PipelineOptions()
        #options = PipelineOptions(flags=[], direct_runner=True, direct_num_workers=1)

        xml_file_path = os.path.join(os.path.dirname(__file__), "feed.xml")
        
        logger.info(f"Iniciando procesamiento de archivo XML: {xml_file_path}")
        
        # Cargar datos del XML
        xml_data = parse_xml(xml_file_path)
        
        if not xml_data:
            logger.error("No se encontraron datos en el XML")
            return
        
        logger.info(f"Se encontraron varios {len(xml_data)} registros en el XML")

        # Procesar primero un conjunto pequeño para prueba
        #test_data = xml_data[:10]  # Primeros 10 registros de prueba
        logger.info("Procesando primeros 10 registros para prueba")

        with beam.Pipeline(options=options) as p:
            (p 
             | "Crear PCollection" >> beam.Create(xml_data)
             | "Transformar Datos" >> beam.ParDo(TransformData())
             | "Insertar en MySQL" >> beam.ParDo(InsertDataToMySQL())
            )
            
        logger.info("Pipeline ejecutado exitosamente")

    except Exception as e:
        logger.error(f"Error en la ejecución del pipeline: {e}")
        raise

if __name__ == "__main__":
    run()
