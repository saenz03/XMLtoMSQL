import xml.etree.ElementTree as ET

# Cargar el archivo XML
tree = ET.parse(r"C:\Development\Habi\venv5\src\feed.xml")  # Reemplázalo con el nombre real del XML
root = tree.getroot()

# Mostrar los primeros elementos
for elem in root[:1]:  # Mostrar solo los primeros 5 para revisar
    print(ET.tostring(elem, encoding="utf-8").decode("utf-8"))
