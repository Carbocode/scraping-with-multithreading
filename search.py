import requests
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Insieme globale per tracciare gli ID unici
processed_vets = set()
lock = threading.Lock()

def fetchJson(city, i):
    api_base_url = "https://www.paginegialle.it/ricerca/Veterinario"
    api_url = f"{api_base_url}/{city}/p-{i}?output=json"

    try:
        response = requests.get(api_url, {"Accept": "application/json"})
        response.raise_for_status()  # Gestisce risposte non 200
        return response.json()
    except requests.RequestException as e:
        error_code = e.response.status_code if e.response else 'Nessuna risposta'
        print(f"Errore durante la richiesta a {city}, ricerca terminata a pagina {i}: {error_code}")
        return None

def getData(item):
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
            "0": item['time'].get('2', None),
            "1": item['time'].get('3', None),
            "2": item['time'].get('4', None),
            "3": item['time'].get('5', None),
            "4": item['time'].get('6', None),
            "5": item['time'].get('7', None),
            "6": item['time'].get('1', None), # la settimana loro parte dalla Domenica
        }

    if item.get('ds_ls_telefoni', None) is not None:
        result['Telefono'] = item['ds_ls_telefoni'][0]

    if item.get('ds_ls_telefoni_whatsapp', None) is not None:
        result['Telefono'] = item['ds_ls_telefoni_whatsapp'][0]
    
    return result

def isDuplicate(vet_id):
     with lock:  # Assicura l'accesso thread-safe all'insieme
        if vet_id not in processed_vets:
            processed_vets.add(vet_id)
            return True
        else:
            return False

def scrapeVets(city):
    vets = []

    i = 0
    while (response_data := fetchJson(city, i)) is not None:
        i+=1
        # Verifico la presenza di risposte
        if 'list' in response_data and 'out' in response_data['list'] and 'base' in response_data['list']['out']  and 'results' in response_data['list']['out']['base']:
            for item in response_data['list']['out']['base']['results']:
                vet = getData(item)
                if not isDuplicate(vet['ID']):
                    vets.append(vet)

    with open(f'results/{city}.json', 'w') as file_output:

        json.dump(vets, file_output)
    

# Carica il file JSON
with open('cities.json', 'r') as file_input:
    data = json.load(file_input)

# Assumi che il tuo file JSON abbia un campo 'cities' che è un array di nomi di città
cities = data['cities']

# Itera su ogni città nell'array
with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrapeVets, city): city for city in cities}

        for future in as_completed(futures):
            city = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Errore nel thread per la città {city}: {e}")