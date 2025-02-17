import mysql.connector
from mysql.connector import Error

def check_write_permissions():
    try:
        # Configuración de la conexión a la base de datos
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='habi_user',
            password='habi_pass',
            database='habi_db'
        )

        if connection.is_connected():
            print("Conexión a la base de datos establecida correctamente.")

            cursor = connection.cursor()

            # Insertar un registro en la tabla 'usuarios'
            insert_usuario_query = """
            DROP table usuarios;
            """
            '''cursor.execute(insert_usuario_query)
            usuario_id = cursor.lastrowid  # Obtener el ID del usuario insertado
            connection.commit()  # Confirmar la transacción
            print("Registro insertado correctamente en la tabla 'usuarios'.")

            # Insertar un registro en la tabla 'propiedades'
            insert_propiedad_query = """
            INSERT INTO propiedades (state, city, colony, street, external_num, code, type, user_id)
            VALUES ('State1', 'City1', 'Colony1', 'Street1', '123', 'CODE123', 'Casa', %s);
            """
            cursor.execute(insert_propiedad_query, (usuario_id,))
            connection.commit()  # Confirmar la transacción
            print("Registro insertado correctamente en la tabla 'propiedades'.")

            # Verificar los registros insertados
            cursor.execute("SELECT * FROM usuarios WHERE id = %s;", (usuario_id,))
            usuario_record = cursor.fetchone()
            if usuario_record:
                print("Registro encontrado en 'usuarios':", usuario_record)
            else:
                print("El registro no se encontró en la tabla 'usuarios'.")

            cursor.execute("SELECT * FROM propiedades WHERE user_id = %s;", (usuario_id,))
            propiedad_record = cursor.fetchone()
            if propiedad_record:
                print("Registro encontrado en 'propiedades':", propiedad_record)
            else:
                print("El registro no se encontró en la tabla 'propiedades'.")
'''
    except Error as e:
        print(f"Error al conectar o escribir en la base de datos: {e}")
   

if __name__ == "__main__":
    check_write_permissions()