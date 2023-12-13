import os
import json

# Percorso della cartella contenente i file JSON
cartella = 'results'

# Array finale per contenere tutti i dati unificati
dati_unificati = []

# Set per tenere traccia degli ID unici
id_unici = set()
id_duplicati = set()

# Ciclo per leggere ogni file nella cartella
for filename in os.listdir(cartella):
    if filename.endswith('.json'):
        percorso_file = os.path.join(cartella, filename)
        with open(percorso_file, 'r') as file:
            dati = json.load(file)
            for elemento in dati:
                id_elemento = elemento.get("ID")
                if id_elemento in id_unici:
                    id_duplicati.add(id_elemento)
                else:
                    id_unici.add(id_elemento)
                    dati_unificati.append(elemento)

# Stampa il totale degli elementi uniti
print(f"Totale elementi uniti: {len(dati_unificati)}")

# Stampa il numero e gli ID duplicati, se presenti
if id_duplicati:
    print(f"Totale ID duplicati trovati: {len(id_duplicati)}")
    print(f"ID Duplicati: {id_duplicati}")
else:
    print("Nessun ID duplicato trovato.")

# Salvare i dati unificati in un nuovo file JSON
with open('dati_unificati.json', 'w') as file_output:
    json.dump(dati_unificati, file_output)

print("Dati unificati salvati in 'dati_unificati.json'")
