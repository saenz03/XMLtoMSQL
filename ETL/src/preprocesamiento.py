import xml.etree.ElementTree as ET
import os
import logging
from  lxml  import  etree 

def safe_get_text(element):
    """Devuelve el texto del elemento XML o None si no existe."""
    return element.text.strip() if element is not None and element.text else None

def parse_xml(file_path):
    """Parses XML and extracts relevant property and user data."""
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo XML no encontrado: {file_path}")

        tree = ET.parse(file_path)
        root = tree.getroot()

        listings = []
        for listing in root.findall("listing"):
            property_data = {
                "state": safe_get_text(listing.find("state")),
                "city": safe_get_text(listing.find("city")),
                "colony": safe_get_text(listing.find("colony")),
                "street": safe_get_text(listing.find("street")),
                "external_num": safe_get_text(listing.find("external_num")),
                "code": safe_get_text(listing.find("code")),
                "type": safe_get_text(listing.find("type")),
                "purpose": safe_get_text(listing.find("purpose")),
                "price": float(safe_get_text(listing.find("price"))) if safe_get_text(listing.find("price")) else None,
                "mail_contact": safe_get_text(listing.find("mail_contact")),
                "phone_contact": safe_get_text(listing.find("phone_contact")),
            }
            listings.append(property_data)
            #logging.info(f"Se encontraron {len(listings)} registros en el XML.")

        return listings

    except Exception as e:
        print(f"Error parsing XML: {e}")
        return []


    
if __name__ == "__main__":
    # Construimos la ruta absoluta del archivo XML
    XML_FILE = os.path.join(os.path.dirname(__file__), "feed.xml")
    
    properties = parse_xml(XML_FILE)
    
    if properties:
        #print("hola")
        for prop in properties:
            print(prop)
    else:
        print("No se encontraron propiedades en el XML.")
