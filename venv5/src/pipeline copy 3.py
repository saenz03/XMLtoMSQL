import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import mysql.connector
from mysql.connector import Error
from preprocesamiento import parse_xml  # type: ignore # Suponemos que esta función ya está definida para procesar el XML
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class TransformData(beam.DoFn):
    """Clase que toma cada diccionario de datos y lo transforma para Beam"""
    def process(self, element):
        # Aquí podemos agregar más validaciones si es necesario
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
        try:
            # Conexión con MySQL
            connection = mysql.connector.connect(
                host='mysql',  # Nombre del servicio en Docker
                database='habi_db',  # Nombre de la base de datos
                user='habi_user',  # Usuario
                password='habi_pass'  # Contraseña
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
                INSERT INTO properties (state, city, colony, street, external_num, code, type, purpose, price, mail_contact, phone_contact, user_id)
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
            if connection.is_connected():
                cursor.close()
                connection.close()



 



def run():
    """Función que configura y ejecuta el pipeline"""
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        # Ruta dinámica del archivo XML
        xml_file_path = os.path.join(os.path.dirname(__file__), "feed.xml")
        
        # Procesamos el XML con la función ya validada (parse_xml debería devolver los datos procesados)
        xml_data = parse_xml(xml_file_path)  
        
        # Creamos una PCollection con los datos procesados del XML
        input_data = p | "Create Input PCollection" >> beam.Create(xml_data)
        
        # Aplicamos la transformación a los datos
        transformed_data = input_data | "Transform Data" >> beam.ParDo(TransformData())
        
        # Insertamos los datos transformados en MySQL
        transformed_data | "Insert Data into MySQL" >> beam.ParDo(InsertDataToMySQL())

if __name__ == "__main__":
    run()
