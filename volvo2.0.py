import os
import pandas as pd
import zipfile
import shutil
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime

# Cargar variables de entorno desde un archivo .env
load_dotenv()

# Obtener rutas desde variables de entorno
WORKNG_PATH = os.getenv("WORKNG_PATH")
SANDBX_PATH = os.getenv("SANDBX_PATH")
DOWNLOADS_PATH = os.path.expanduser("~/Downloads")

USERNAME = "aldo.quintal@simdatagroup.com"
PASSWORD = "Lynx2021."
URL = "https://simetrical.net/"
BOVEDA_URL = "https://simetrical.net/simetrical/boveda/?q=ODM4MQ=="


if not WORKNG_PATH or not SANDBX_PATH:
    raise ValueError("Las variables de entorno 'WORKNG_PATH' y 'SANDBX_PATH' deben estar definidas en el archivo .env")



# Configurar Selenium WebDriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Para ejecutar en segundo plano
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

def login_to_site():
    driver.get(URL)
    time.sleep(2)
    
    # Mostrar ventana del navegador
    driver.maximize_window()
    
    # Buscar y completar el campo de usuario
    username_field = driver.find_element(By.ID, "txtuser")
    username_field.send_keys(USERNAME)
    
    # Buscar y completar el campo de contraseña
    password_field = driver.find_element(By.ID, "txtpassword")
    password_field.send_keys(PASSWORD)
    password_field.send_keys(Keys.RETURN)
    
    time.sleep(3)
    if "dashboard" in driver.current_url:
        print("Inicio de sesión exitoso")
    else:
        print("Descargando ...")

def navigate_to_boveda():
    driver.get(BOVEDA_URL)
    time.sleep(3)
    
    # Seleccionar el año y mes actuales
    current_year = datetime.today().strftime('%Y')
    current_month = datetime.today().strftime('%m')
    
    year_link = driver.find_element(By.LINK_TEXT, current_year)
    year_link.click()
    time.sleep(3)
    
    month_link = driver.find_element(By.LINK_TEXT, current_month)
    month_link.click()
    time.sleep(3)
    
    print("Navegación a la bóveda completada")

def download_zip():
    today = datetime.today().strftime('%d')  # Obtener el día actual
    zip_filename = f"838101{today}.zip"
    zip_path = os.path.join(DOWNLOADS_PATH, zip_filename)
    
    try:
        zip_link = driver.find_element(By.PARTIAL_LINK_TEXT, zip_filename)
        zip_link.click()
        print(f"Descargando {zip_filename}")
        
        time.sleep(10)  # Esperar la descarga
        
        if os.path.exists(zip_path):
            shutil.move(zip_path, WORKNG_PATH)
            print(f"Archivo movido a {WORKNG_PATH}")
        else:
            print("No se encontró el archivo en la carpeta de descargas")
    except:
        print(f"No se encontró el archivo {zip_filename}")
    
login_to_site()
navigate_to_boveda()
download_zip()

time.sleep(5)  # Mantener la ventana abierta por un tiempo para ver el estado

print("Proceso completado")


# Limpiar la carpeta 3-Sandbx antes de descomprimir
for file in os.listdir(SANDBX_PATH):
    file_path = os.path.join(SANDBX_PATH, file)
    if os.path.isfile(file_path) or os.path.isdir(file_path):
        os.remove(file_path) if os.path.isfile(file_path) else os.rmdir(file_path)

# Buscar el archivo ZIP en 2-Workng
zip_files = [f for f in os.listdir(WORKNG_PATH) if f.endswith('.zip')]
if not zip_files:
    raise FileNotFoundError("No se encontró ningún archivo .zip en la carpeta 2-Workng")

zip_file_path = os.path.join(WORKNG_PATH, zip_files[0])

# Extraer archivos en 3-Sandbx
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(SANDBX_PATH)

# Obtener el archivo descomprimido
extracted_files = [f for f in os.listdir(SANDBX_PATH) if f.endswith('.xlsx')]
if not extracted_files:
    raise FileNotFoundError("No se encontró ningún archivo .xlsx en la carpeta 3-Sandbx después de descomprimir")

FILE_PATH = os.path.join(SANDBX_PATH, extracted_files[0])

def extract_and_process_headers(excel_file, sheet_names):
    all_headers = set()
    
    for sheet in sheet_names:
        sheet_data = pd.read_excel(excel_file, sheet_name=sheet, header=None)
        
        if len(sheet_data) > 6:
            headers = sheet_data.iloc[6, 1:].dropna().values  
            all_headers.update(headers)  
    
    processed_headers = sorted(set(str(header).replace('.0', '') for header in all_headers))
    return processed_headers

def process_rows_from_sheet_with_mes(excel_file, sheet_name, output_df, columns):
    sheet_data = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
    
    mes_value = sheet_data.iloc[3, 1]  
    if pd.isna(mes_value):
        raise ValueError(f"La celda B4 en la hoja '{sheet_name}' no contiene un valor válido.")
    
    try:
        year, month = mes_value.split()  
        month_number = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
            "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
        }[month]
        mes_numeric = f"{year[-2:]}{month_number}"
    except Exception as e:
        raise ValueError(f"Error al procesar el valor de la celda B4 en la hoja '{sheet_name}': {mes_value}") from e
    
    start_row = sheet_data[sheet_data.eq("Row Labels").any(axis=1)].index[0] + 1
    
    new_rows = []
    
    for _, row in sheet_data.iloc[start_row:].iterrows():
        dealer_net_sales = row[0]  
        if pd.isna(dealer_net_sales) or str(dealer_net_sales).strip() == "Grand Total":
            continue  
        
        new_row = {"Dealer_Net_Sales": dealer_net_sales}
        
        for col in columns[1:-1]:  
            col_to_match = col 
            try:
                matching_index = sheet_data.iloc[6].astype(str).str.replace('.0', '').tolist().index(str(col_to_match))
                new_row[col] = row[matching_index] if not pd.isna(row[matching_index]) else None
            except ValueError:
                new_row[col] = None
        
        new_row["Mes_"] = mes_numeric
        
        new_rows.append(new_row)
    
    if new_rows:
        output_df = pd.concat([output_df, pd.DataFrame(new_rows)], ignore_index=True)
    
    return output_df

def process_all_sheets_with_mes(excel_file, sheet_names, columns):
    output_df = pd.DataFrame(columns=columns)  
    
    for sheet_name in sheet_names:
        output_df = process_rows_from_sheet_with_mes(excel_file, sheet_name, output_df, columns)
    
    return output_df

sheet_names = pd.ExcelFile(FILE_PATH).sheet_names 
output_csv_path = os.path.join(SANDBX_PATH, 'pgnetsal.csv')

headers = extract_and_process_headers(FILE_PATH, sheet_names)
columns = ["Dealer_Net_Sales"] + headers + ["Mes_"]

output_df = process_all_sheets_with_mes(FILE_PATH, sheet_names, columns)

output_df.to_csv(output_csv_path, index=False)

# Limpiar la carpeta 2-Workng después de procesar
for file in os.listdir(WORKNG_PATH):
    file_path = os.path.join(WORKNG_PATH, file)
    if os.path.isfile(file_path) or os.path.isdir(file_path):
        os.remove(file_path) if os.path.isfile(file_path) else shutil.rmtree(file_path)

print(f"Archivo procesado en {output_csv_path}")
