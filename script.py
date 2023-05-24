import os
import time
import pandas as pd
from confluent_kafka import Producer

def process_csv(file_path, cities_dict):
    # Carica il file CSV utilizzando Pandas
    df = pd.read_csv(file_path)

    # Aggiungi le colonne 'City' e 'Zip' basate sulla corrispondente città nel dizionario
    df['Zip'] = df['City'].map(cities_dict)

    # Seleziona solo le colonne desiderate
    new_df = df[['Fullname', 'City', 'Zip']].copy()

    # Dividi il campo 'Fullname' in 'Firstname' e 'Lastname'
    new_df[['Firstname', 'Lastname']] = df['Fullname'].str.split(' ', n=1, expand=True)

    # Riordina le colonne nel nuovo DataFrame
    new_df = new_df[['Firstname', 'Lastname', 'City', 'Zip']].copy()

    # Crea il nuovo file CSV
    new_file_path = file_path.replace('.csv', '_new.csv')
    new_df.to_csv(new_file_path, index=False)

    # Pubblica i dati su Apache Kafka
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    for _, row in new_df.iterrows():
        message = ','.join([str(row[field]) for field in new_df.columns])
        producer.produce('csvcitta', value=message.encode('utf-8'))

    producer.flush()

    print(f"Il file {file_path} è stato elaborato, i dati sono stati pubblicati su Apache Kafka e il nuovo file CSV è stato creato.")

# Ottieni il percorso assoluto dello script Python corrente
script_dir = os.path.dirname(os.path.abspath(__file__))

# File delle città da leggere come dizionario in memoria
cities_file = os.path.join(script_dir, 'indirizzi.csv')
cities_dict = {}

# Verifica l'esistenza del file delle città
if os.path.exists(cities_file):
    with open(cities_file, 'r') as file:
        for line in file:
            city, zip_code = line.strip().split(',')
            cities_dict[city] = zip_code
else:
    print("File delle città non trovato.")

# Monitora la directory corrente per nuovi file
while True:
    files = os.listdir(script_dir)
    for file in files:
        file_path = os.path.join(script_dir, file)
        if file == 'persone.csv':
            process_csv(file_path, cities_dict)
            os.remove(file_path)  # Rimuovi il file dopo averlo elaborato
    time.sleep(5)  # Attendere 5 secondo prima di controllare nuovamente la directory
