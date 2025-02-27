import os
import zipfile
import mysql.connector
from dotenv import load_dotenv
from twilio.rest import Client
import time
from datetime import datetime

load_dotenv()

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "M0nSim3tric4l",
    "database": "crm_simetrical"
}

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = "whatsapp:+14155238886" 
DESTINATARIO_WHATSAPP = "whatsapp:+5219997006060" 

CODIGO_SANDBOX = "join organization-sink" 

ZIP_PATH = os.getenv("ZIP_PATH", "R:\\DelDia")

TAMANO_UMBRAL = 1024 

def activar_sandbox():
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        client.messages.create(
            from_=TWILIO_WHATSAPP_NUMBER,
            body=CODIGO_SANDBOX,
            to=TWILIO_WHATSAPP_NUMBER
        )
        print("✅ Sandbox reactivada automáticamente.")
        time.sleep(10)  
    except Exception as e:
        print(f"⚠️ Error al activar la sandbox: {e}")

def enviar_notificacion_whatsapp(mensaje):
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        message = client.messages.create(
            from_=TWILIO_WHATSAPP_NUMBER,
            body=mensaje,
            to=DESTINATARIO_WHATSAPP
        )
        print(f"📩 Mensaje enviado: {message.sid}")
    except Exception as e:
        print(f"⚠️ Error al enviar el mensaje de WhatsApp: {e}")

def obtener_marca_cliente(client_id):
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)
        
        query = "SELECT made FROM clients WHERE Client = %s"
        cursor.execute(query, (client_id,))
        results = cursor.fetchall() 

        cursor.close()
        connection.close()

        if results:
            made_value = results[0]["made"]
            if made_value in ["Suzuki", "JAC"]:
                return made_value

        return None

    except mysql.connector.Error as err:
        print(f"⚠️ Error en la base de datos: {err}")
        return None

def validar_zips_vacios():
    zips_vacios_suzuki_jac = []
    
    for zip_file in os.listdir(ZIP_PATH):
        if zip_file.endswith(".zip"):
            zip_path = os.path.join(ZIP_PATH, zip_file)
            
            client_id = str(int(zip_file[:4]))

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                archivos = zip_ref.namelist()
                
                if not archivos or sum(zip_ref.getinfo(f).file_size for f in archivos) < TAMANO_UMBRAL:
                    marca = obtener_marca_cliente(client_id)
                    
                    if marca:
                        zips_vacios_suzuki_jac.append((zip_file, marca))
    
    if zips_vacios_suzuki_jac:
        mensaje = "🚨 *ALERTA*: ZIPs vacíos detectados para Suzuki o JAC:\n"
        for zip_vacio, marca in zips_vacios_suzuki_jac:
            mensaje += f"- {zip_vacio} ({marca})\n"
    else:
        mensaje = f"✅ *Todo en orden*: No se encontraron ZIPs vacíos de Suzuki o JAC. ({datetime.now().strftime('%Y-%m-%d')})"

    enviar_notificacion_whatsapp(mensaje)

if __name__ == "__main__":
    activar_sandbox() 
    validar_zips_vacios()
