import requests
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

categoria = "Veterinaria%20-%20ambulatori%20e%20laboratori" # categoria ricercata
processed_results = set() # Collection globale per tracciare tutti gli ID trovati
lock = threading.Lock() # permette di fermare tutti i thread per non avere concorrenza durante l'accesso ad una risorsa


def fetchJson(city, i):
    """
    Aggiunge un nuovo animale all'ambulatorio.

    Args:
        name (str): Il nome dell'animale.
        species (str): La specie dell'animale.

    Returns:
        dict: Dictionary contenente il risultato della chiamata
    """
    api_base_url = "https://www.paginegialle.it/ricerca"
    api_url = f"{api_base_url}/{categoria}/{city}/p-{i}?output=json"

    try:
        response = requests.get(api_url, {"Accept": "application/json"})
        response.raise_for_status()  # Gestisce risposte non 200
        return response.json()
    except requests.RequestException as e:
        error_code = e.response.status_code if e.response else 'Nessuna risposta'
        print(f"Errore durante la richiesta a {city}, ricerca terminata a pagina {i}: {error_code}")
        return None

def splitShifts(shifts):
    if shifts is not None:
        return [(shift.split(" - ")[0], shift.split(" - ")[1]) for shift in shifts if " - " in shift]
    else:
        return None

def getData(item):
    """
    Prende il singolo risultato ottenuto dalla chiamata e li formatta prelevando solo i dati utili

    Args:
        item (dict): Dictionary contenente tutti i dati di un singolo risultato

    Returns:
        dict: Restituisce tutti i dati salienti formattati
    """

    result = {
        "ID": item.get('cd_id_sede', None),
        "Nome": item.get('ds_ragsoc', None),
        "Descrizione" : item.get('ds_abstract', None),
        "Latitudine": item.get('nr_lat', None),
        "Longitudine": item.get('nr_long', None),
        "Citta": item.get('loc', None),
        "OrdineProvinciale": item.get('prov', None),
        "Indirizzo": item.get('addr', None)
    }

    if item.get('ds_ls_mail', None) is not None:
        result['Email'] = item['ds_ls_mail'].get('0', None)

    if item.get('time', None) is not None:
        result['Orari'] = {
            "0": splitShifts(item['time'].get('2', None)),
            "1": splitShifts(item['time'].get('3', None)),
            "2": splitShifts(item['time'].get('4', None)),
            "3": splitShifts(item['time'].get('5', None)),
            "4": splitShifts(item['time'].get('6', None)),
            "5": splitShifts(item['time'].get('7', None)),
            "6": splitShifts(item['time'].get('1', None)), # la settimana del sito parte dalla Domenica
        }

    telefoni = item.get('ds_ls_telefoni')
    if telefoni and len(telefoni) > 0:
        result['Telefono'] = telefoni[0]

    telefoni_whatsapp = item.get('ds_ls_telefoni_whatsapp')
    if telefoni_whatsapp and len(telefoni_whatsapp) > 0:
        result['Telefono'] = telefoni_whatsapp[0]
    
    return result

def isDuplicate(result_id):
    """ 
    Controlla se l'ID è duplicato

    Args:
        item (str): ID del risultato che si vuole controllare

    Returns:
        bool: se è duplicato o no
    """

    with lock:  # Assicura l'accesso thread-safe all'insieme
        if result_id not in processed_results:
            processed_results.add(result_id)
            return True
        else:
            return False

def scrapeData(city):
    """
    Thread che si occupa di recuperare i dati richiesti in una città

    Args:
        city (str): Città

    Returns:
        void
    """
    results = []

    i = 0
    while (response_data := fetchJson(city, i)) is not None:
        i+=1
        # Verifico la presenza di risposte
        if 'list' in response_data and 'out' in response_data['list'] and 'base' in response_data['list']['out']  and 'results' in response_data['list']['out']['base']:
            for item in response_data['list']['out']['base']['results']:
                result = getData(item)
                if not isDuplicate(result['ID']):
                    results.append(result)

    with open(f'results/{city}.json', 'w') as file_output:

        json.dump(results, file_output)
    
# Carica il file JSON
with open('cities.json', 'r') as file_input:
    data = json.load(file_input)

cities = data['cities']

# Itera su ogni città nell'array
with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrapeData, city): city for city in cities}

        for future in as_completed(futures):
            city = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Errore nel thread per la città {city}: {e}")