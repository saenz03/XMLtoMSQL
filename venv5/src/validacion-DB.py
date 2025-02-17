import mysql.connector
from mysql.connector import Error

def check_database_connection():
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

            # Verificar si las tablas existen
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()

            # Lista de tablas esperadas
            expected_tables = {'usuarios', 'propiedades'}
            existing_tables = {table[0] for table in tables}

            if expected_tables.issubset(existing_tables):
                print("Las tablas 'usuarios' y 'propiedades' se crearon correctamente.")
            else:
                print("Faltan algunas tablas. Tablas existentes:", existing_tables)

            # Verificar acceso a la base de datos
            cursor.execute("SELECT COUNT(*) FROM usuarios;")
            user_count = cursor.fetchone()[0]
            print(f"Número de usuarios en la tabla 'usuarios': {user_count}")

            cursor.execute("SELECT COUNT(*) FROM propiedades;")
            property_count = cursor.fetchone()[0]
            print(f"Número de propiedades en la tabla 'propiedades': {property_count}")

    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    check_database_connection()