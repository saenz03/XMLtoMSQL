import mysql.connector

try:
    connection = mysql.connector.connect(
        host="localhost",  # O el nombre del servicio de Docker
        user="habi_user",
        password="habi_pass",
        database="habi_db"
    )

    if connection.is_connected():
        print("✅ Conexión exitosa a MySQL")
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        print("📌 Tablas existentes:", tables)

        cursor.execute("SELECT COUNT(*) FROM usuarios;")
        print("👤 Usuarios en la base de datos:", cursor.fetchone()[0])

        cursor.execute("SELECT COUNT(*) FROM propiedades;")
        print("🏠 Propiedades en la base de datos:", cursor.fetchone()[0])

except mysql.connector.Error as e:
    print(f"❌ Error de conexión: {e}")

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
