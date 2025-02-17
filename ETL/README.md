# ETL Pipeline para Procesamiento de Datos XML y Persistencia en MySQL

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) en Python utilizando Apache Beam para procesar un archivo XML, transformar los datos y persistirlos en una base de datos MySQL. El proyecto está diseñado para ejecutarse en contenedores Docker y utiliza Docker Compose para la orquestación.

---

## **Requisitos**

1. **Docker**: Asegúrate de tener Docker instalado en tu sistema.
2. **Docker Compose**: Asegúrate de tener Docker Compose instalado.
3. **Git**: Para clonar y gestionar el repositorio.



## **Instrucciones de Ejecución**

### **1. Clonar el Repositorio**
Clona el repositorio en tu máquina local:


git clone https://github.com/tu-usuario/XMLtoMSQL.git


## Construir y Ejecutar los Contenedores
Ejecuta el siguiente comando para construir y levantar los contenedores:

docker-compose up --build


## Verificar la Base de Datos
Conéctate a la base de datos MySQL para verificar que los datos se hayan insertado correctamente:

docker exec -it mysql_habi mysql -u habi_user -phabi_pass habi_db


## Preguntas de Negocio

1. ¿Cuántos usuarios hay registrados?

SELECT COUNT(*) AS total_usuarios FROM usuarios;

Resultado: 9481


¿Cuántas propiedades hay por cada usuario?

SELECT u.mail_contact, COUNT(p.id) AS total_propiedades
FROM usuarios u
LEFT JOIN propiedades p ON u.id = p.user_id
GROUP BY u.mail_contact;

Resultado:

+--------------------+-------------------+
| mail_contact       | total_propiedades |
+--------------------+-------------------+
| user-4436@mail.com |                 8 |
| user-3349@mail.com |                 8 |
| user-2418@mail.com |                 8 |
| user-923@mail.com  |                 8 |
| user-2821@mail.com |                 8 |
+--------------------+-------------------+

¿Cuántas casas y cuántos departamentos hay por estado?

SELECT state, type, COUNT(*) AS total
FROM propiedades
WHERE type IN ('Casa', 'Departamento')
GROUP BY state, type;

Resultado: 

| state                           | type         | total |
+---------------------------------+--------------+-------+
| Jalisco                         | Departamento |  1407 |
| Aguascalientes                  | Casa         |    50 |
| M�xico                          | Departamento |  1587 |
| DF / CDMX                       | Departamento |  1202 |
| DF / CDMX                       | Casa         |  4314 |
| Jalisco                         | Casa         |   665 |
| Nuevo Le�n                      | Departamento |   995


¿Tenemos códigos duplicados? ¿Por qué?

SELECT code, COUNT(*) AS total
FROM propiedades
GROUP BY code
HAVING total > 1;

Resultado: 

No, no debería haber códigos duplicados en la tabla propiedades. Esto se debe a que el campo code en la tabla propiedades está definido como único (UNIQUE) en la estructura de la base de datos. Además, durante el proceso de preprocesamiento y transformación de datos, se implementó una lógica para evitar la inserción de registros duplicados.
El campo code en la tabla propiedades tiene una restricción UNIQUE, lo que garantiza que no puedan existir dos registros con el mismo valor en este campo. Si se intenta insertar un código duplicado, la base de datos lanzará un error y rechazará la inserción.

## Manejo de Errores
El proyecto incluye manejo de errores para:

Conexión a la base de datos.
Parseo del archivo XML.
Inserción de datos en MySQL.
Los errores se registran en los logs del contenedor etl_habi.

# Dependencias
Las dependencias del proyecto se encuentran en el archivo requirements.txt:

apache-beam[gcp]==2.40.0
mysql-connector-python==8.0.31
lxml==4.9.2


# Contacto
Si tienes alguna pregunta o sugerencia, no dudes en contactarme:

Nombre: Juan Sebastian Chaparro Saenz

Email: Saenz03@gmail.com

GitHub: https://github.com/saenz03
