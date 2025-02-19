import pandas as pd
from datetime import datetime

# Leer un archivo Excel
archivo_excel = 'C:\\Users\\Aldo Quintal\\OneDrive - Simetrical SA de CV\\Escritorio\\Proyctos\\RandyOrton\\PG_General07012025.xlsx'

# Leer todas las pestañas en un diccionario {nombre_pestaña: dataframe}
t_h = pd.read_excel(archivo_excel, sheet_name=None, skiprows=5)
# Obtener el año actual
año_a = datetime.now().year
# Obtener el mes actual
mes_a = datetime.now().strftime("%B")

# Obtener todas las hojas
libros = list(t_h.keys())
dataframes = []

# Mapeo de nombres de meses en inglés a español
meses_map = {
    "January": "Enero",
    "February": "Febrero",
    "March": "Marzo",
    "April": "Abril",
    "May": "Mayo",
    "June": "Junio",
    "July": "Julio",
    "August": "Agosto",
    "September": "Septiembre",
    "October": "Octubre",
    "November": "Noviembre",
    "December": "Diciembre"
}
# Iterar sobre los meses en orden
mes_a_es =meses_map[mes_a]
for mes in meses_map.values():
    if mes in libros:  # Comprobar si el mes está en las hojas
        # Procesar la hoja
        df_hoja = t_h[mes]
        df_hoja = df_hoja.fillna('')
        df_hoja = df_hoja.drop(columns=['Unnamed: 10'], errors='ignore')

        # Renombrar las columnas
        df_hoja = df_hoja.rename(columns={
            'Dealer Net Sales': 'Dealer_Net_Sales',
            'Column Labels': '11',
            'Unnamed: 2': '13',
            'Unnamed: 3': '14',
            'Unnamed: 4': '15',
            'Unnamed: 5': '17',
            'Unnamed: 6': '18',
            'Unnamed: 7': '25',
            'Unnamed: 8': '80',
            'Unnamed: 9': '91'
        })

        # Verificar columnas disponibles
        columns_v = ['Dealer_Net_Sales', '11', '13', '14', '15', '17', '18', '25', '80', '91']
        columns_d = [col for col in columns_v if col in df_hoja.columns]

        # Crear un nuevo DataFrame para las columnas seleccionadas
        ndf = pd.DataFrame({col: df_hoja[col].iloc[2:] for col in columns_d})

        # Calcular la columna con el año y el mes
        mes_indice = list(meses_map.values()).index(mes) + 1  # +1 para que enero sea 1
        ndf['Mes_'] = (año_a % 100) * 100 + mes_indice

        dataframes.append(ndf)

        # Salir del ciclo si el mes actual se ha procesado
        if mes == mes_a_es:
            break

# Comprobar si hay DataFrames para concatenar
if dataframes:
    df_final = pd.concat(dataframes, ignore_index=True)
    # Filtrar el DataFrame para eliminar filas no deseadas
    df_final = df_final[~df_final['Dealer_Net_Sales'].isin(['Row Labels', 'Grand Total'])]  # Excluir Row Labels y Grand Total
    df_final = df_final[df_final['Dealer_Net_Sales'] != '']  # Excluir filas vacías en Dealer_Net_Sales
    print(df_final)
    df_final.to_csv('pgnetsal.csv', index=False, encoding='utf-8')  # Cambia 'df_final.csv' por el nombre que desees
    df_final.to_csv('pgnetsal.txt', index=False, encoding='utf-8', sep='|')
    print("DataFrame exportado a pgnetsal.csv")
else:
    print("No se encontraron hojas para procesar.")
