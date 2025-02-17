# Usar una imagen base con Python y Apache Beam
FROM apache/beam_python3.9_sdk:latest

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar los archivos necesarios
COPY requirements.txt .
COPY src/ /app/

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Comando por defecto para ejecutar el pipeline
#CMD ["python", "pipeline.py"]
CMD ["python", "/app/pipeline.py"]