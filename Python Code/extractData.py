import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import subprocess
import os
import bz2
import gzip
from pathlib import Path

# Carga el archivo CSV en un DataFrame
def load_csv_to_dataframe(folder_drive, file_name):
  # Combina el directorio y el nombre del archivo para obtener la ruta completa
  full_path = folder_drive + file_name
  df = pd.read_csv(full_path)

  return df
  

def extract_conditions(df_source, column_name, sep):
  # Extrae la columna de interés y elimina valores NaN
  conditions_series = df_source[column_name].dropna()

  # Inicializa una lista para guardar todas las condiciones
  all_conditions = []

  # Itera sobre cada entrada, divide por comas y añade a la lista
  for entry in conditions_series:
    # Usa strip() para eliminar espacios en blanco alrededor de cada condición
    conditions = [cond.strip() for cond in entry.split(sep)]
    all_conditions.extend(conditions)

  # Crea un conjunto para obtener solo las condiciones únicas y luego conviértelo a una lista
  unique_conditions = sorted(list(set(all_conditions)))

  return unique_conditions
  
  
def create_conditions_df(df_data, unique_conditions,col_id,col2Extract,sep):
  # Crea el DataFrame df_conditions
  df_conditions = pd.DataFrame({col_id: df_data[col_id].unique()})

  # Inicializa las columnas de condiciones con 0
  for condition in unique_conditions:
      df_conditions[condition] = 0

  # Itera sobre el DataFrame original para rellenar df_conditions
  for index, row in df_data.iterrows():
      participant_id = row[col_id]
      conditions_str = row[col2Extract]

      if pd.notna(conditions_str):
          # Divide las condiciones y limpia espacios
          participant_conditions = [cond.strip() for cond in conditions_str.split(sep)]

          # Actualiza el DataFrame df_conditions
          for cond in participant_conditions:
              if cond in df_conditions.columns:
                  df_conditions.loc[df_conditions[col_id] == participant_id, cond] = 1

  return df_conditions
  
  
# Dada una url, obtiene la url a la que es redirigida
# También devuelve si es un fichero text/html parseable
def get_real_url(original_url):
    try:
        response = requests.get(original_url, allow_redirects=True, timeout=10)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        final_url = response.url

        # Obtenemos el Content-Type del encabezado de la respuesta
        content_type = response.headers.get('Content-Type', '').lower()
        print(content_type)

        # Verificamos si el Content-Type es HTML. Si no lo es, y no es XHTML,
        # asumimos que no es adecuado para html.parser.
        if 'text/html' not in content_type and not content_type.startswith('application/xhtml'):
          print(f"Skipping: Content-Type '{content_type}' is not HTML. Not suitable for html.parser.")
          return final_url, response, False # Devolvemos None para indicar que no se pudo parsear como HTML

        # Verificación adicional por extensión de archivo en la URL (menos robusta que Content-Type)
        # Esto ayuda a identificar posibles archivos grandes no HTML que podrían no tener un Content-Type perfecto
        path_part = original_url.split('?')[0].split('#')[0] # Elimina la query string y el fragmento
        if path_part.endswith(('.zip', '.rar', '.gz', '.tar', '.pdf', '.docx', '.xlsx', '.pptx', '.jpg', '.png', '.mp4', '.mp3')):
            print(f"Skipping: URL extension '{path_part.split('.')[-1]}' suggests non-HTML content. Not suitable for html.parser.")
            return final_url, response, False

        print("Successfully fetched and parsed HTML.")

        return final_url, response, True
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return original_url, None, False
        
        
# Obtiene una página html parseada
def get_html_parser(original_url):
    print(f"Attempting to fetch and parse: {original_url}")
    filename = None
    try:
        final_url, response, is_html = get_real_url(original_url)
      
        # Trata de buscar un nombre de un archivo
        # Intenta obtener el nombre del archivo del encabezado Content-Disposition
        content_disposition = response.headers.get('Content-Disposition')
        if content_disposition:
            # Expresión regular para encontrar filename* o filename
            fname_match = re.search(r'filename\*?=(?:UTF-8\'\'|")?([^\";]+)"?', content_disposition, re.IGNORECASE)
            if fname_match:
                filename = fname_match.group(1).strip()
                # Decodificar caracteres codificados en URL si los hay (ej. %20 a espacio)
                filename = requests.utils.unquote(filename)

        # Si no se encuentra el nombre del archivo en Content-Disposition, extráelo de la ruta de la URL
        if not filename:
            # Elimina los parámetros de consulta y fragmentos antes de obtener el nombre base
            filename = os.path.basename(final_url.split('?')[0].split('#')[0])

        # Si después de todo, el nombre del archivo sigue siendo genérico o vacío, proporciona un nombre de respaldo
        if not filename or filename == '/' or filename == '.':
            filename = None # Nombre de respaldo genérico

        if is_html:
          # Si el Content-Type es HTML, o no hay una clara indicación de lo contrario, procedemos
          soup = BeautifulSoup(response.text, 'html.parser')

          # Heurística adicional: Si después de parsear, no se encuentran etiquetas HTML o BODY,
          # podría ser un documento HTML muy mal formado o no HTML disfrazado.
          #if not soup.find('html') and not soup.find('body'):
          #    print(f"Warning: Content-Type was '{content_type}', but basic HTML tags (<html>, <body>) not found. May not be valid HTML.")
              # Podrías decidir devolver None aquí también, dependiendo de tu tolerancia
              # return final_url, None
        else:
          return final_url, filename, None

        print("Successfully fetched and parsed HTML.")
        return final_url, filename, soup

    except requests.exceptions.Timeout:
        print(f"Error: Request timed out for {original_url}.")
        return original_url, filename, None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching {original_url}: {e}")
        return final_url, filename, None
    except Exception as e: # Captura otros posibles errores durante el parseo de BeautifulSoup
        print(f"An error occurred during HTML parsing for {original_url}: {e}")
        return final_url, filename, None



# Obtiene el listado de ficheros a descargar
# Muy personalizado para esta web
def get_list_genetic_data(web_html, web_base):
    url_list = []

    # 1. Localiza el encabezado o la sección que contenga el texto 'File Listing'
    # Se encuentra bajo un encabezado  <h3> o similar
    file_listing_header = web_html.find('h2', string=lambda text: text and 'File Listing' in text)

    if file_listing_header:
        print("Found 'File Listing' section.")
        #La lista de directorios se encuentra bajo una lista enumerada
        container = file_listing_header.find_next_sibling(['div', 'ul'])

        if container:
            # 2. Busca todos los elementos <a> (enlaces) dentro de esa sección.
            links = container.find_all('a')

            if links:
                print(f"Found {len(links)} potential links in 'File Listing'.")
                for link in links:
                    href = link.get('href')
                    print(href)
                    if href:
                        # 4. Verifica si el href es un enlace absoluto. Si es relativo, construye la URL absoluta.
                        if href.startswith('http://') or href.startswith('https://'):
                            full_url = href
                        elif href.startswith('/'):
                            full_url = web_base.rstrip('/') + href
                        elif href.startswith('.'):
                            full_url =  web_base.rstrip('/') + href.lstrip('.')
                        else:
                            # Handle other relative paths if necessary, or skip
                            continue
                    else:
                        # Handle other relative paths if necessary, or skip
                        continue
                    url_list.append(full_url)
            else:
                # 7. Si se encuentra la sección pero no se encuentran enlaces
                print("No <a> tags found within the 'File Listing' section's container.")
        else:
            print("No suitable container (div/table) found after 'File Listing' header.")
    else:
        # 6. Si no se encuentra la sección 'File Listing'
        print("No 'File Listing' section found in the HTML content.")

    return url_list
    
 
# Dada la lista de participantes (casos + control) se procede a ver los archivos disponibles
# para cada participante para determinar la URL original
def get_download_file(url,target_dir,filename):

    # Si no está el directorio creado lo crea
    os.makedirs(target_dir, exist_ok=True) # Ensure directory exists

    print(f"\n--- Initiating download for specific file ---")
    print(f"Attempting to download {url} to {target_dir}")
    try:
        if filename:
            # Construct the full path for the output file
            full_output_path = os.path.join(target_dir, filename)
            print(f"Saving as: {full_output_path}")
            # Use -O with the full path, and remove -P as it's not needed for -O
            wget_cmd  = ['wget', '-O', full_output_path, url]
        else:
            # If no specific filename is provided, use -P to specify the directory
            print(f"Saving to directory: {target_dir}")
            wget_cmd = ['wget', '-P', target_dir, url]

        process = subprocess.run(wget_cmd, capture_output=True, text=True, check=False)

        if process.returncode == 0:
            print(f"SUCCESS: File downloaded to {target_dir}.")
            # print(f"Wget stdout:\n{process.stdout}") # Optional: show wget output
            return(True)
        else:
            print(f"FAILURE: Failed to download {url}. Error code: {process.returncode}")
            if process.stdout:
                print("Wget stdout:\n" + process.stdout)
            if process.stderr:
                print("Wget stderr:\n" + process.stderr)
            return(False)

    except Exception as e:
        print(f"ERROR: An exception occurred while executing wget: {e}")
        print(f"--- Download attempt finished ---")
        return(False)



def open_compressed_file(path, mode="rt"):
    """
    Abre ficheros .gz, .bz2 o planos con la misma interfaz.
    """
    path = str(path)
    if path.endswith(".gz"):
        return gzip.open(path, mode)
    if path.endswith(".bz2"):
        return bz2.open(path, mode)
    return open(path, mode)
