#!/usr/bin/python3
from pymongo import MongoClient
from datetime import datetime,  timedelta
from urllib.parse import quote_plus
from dotenv import load_dotenv
import csv
import os
import pymongo
import logging
import time
import functools
import argparse

load_dotenv()

MONGO_DB = os.getenv('DB')
MONGO_HOST = os.getenv('DB_HOST')
MONGO_USER = os.getenv('DB_USER')
MONGO_PASS = os.getenv('PASS')
MONGO_MECHANISM = os.getenv('MECHANISM')
MAX_AUTO_RECONNECT_ATTEMPTS = 5

uri = f'mongodb://{quote_plus(MONGO_USER)}:{quote_plus(MONGO_PASS)}@{MONGO_HOST}/{MONGO_DB}'
MAX_AUTO_RECONNECT_ATTEMPTS = 5
client = MongoClient(uri, socketTimeoutMS=90000000, connectTimeoutMS=90000000)
db = client.twitterdb
collection = db.tweetsMexico
conceptos={
    'esquite':['esquite', 'trolelote', 'chasca', 'elote en vaso', 'vasolote', 'elote feliz', 'coctel de elote', 'elote desgranado'], 
    'bolillo':['bolillo', 'birote'], 
    'migaja':['migaja', 'borona', 'morona', 'morusa'], 
    'queso Oaxaca':['queso Oaxaca', 'quesillo', 'queso de hebra'], 
    'hormiga':['hormiga', 'asquel', 'asquilin'], 
    'mosquito':['mosquito','zancudo','chaquiste','chanquiste','moyote'],
    'pavo':['pavo', 'guajolote', 'totole', 'totol', 'chompipe'], 
    'colibrí':['colibri', 'chupamirto', 'chuparrosa', 'chupaflor'], 
    'automóvil':['coche', 'automovil', 'carro', 'auto'], 
    'aguacero':['aguacero', 'chubasco', 'tormenta'], 
    'habitación':['habitacion', 'alcoba', 'dormitorio', 'recamara'], 
    'cobija':['cobija', 'frazada'], 
    'lentes':['lentes', 'anteojo', 'gafas', 'espejuelos'], 
    'rasguño':['rasguño', 'arañazo'], 
    'lagaña':['legaña', 'lagaña', 'chinguiña'], 
    'comezón':['comezon', 'picazon', 'rasquera', 'rasquiña'],  
    'cinturón':['cinturon', 'cinto', 'fajo'],  #(bucar fajo con opción “sin billetes” en la expresión regular)
    'escusado':['retrete', 'escusado', 'inodoro', 'WC'], 
    'brasier':['brasier', 'chichero']  
}

HOME = os.environ["HOME"]
workdir=os.path.join(HOME,"repos/dialectic-twitter-maps-generator")
datadir=os.path.join(workdir,'data')

def make_query(term):
    match term:
        case "chasca":
            regex = r"\bchas[ck]?a[s]?[\!\?\,\.]?\b"
        case "elote en vaso":
            regex = r"\belote[s]?\sen\svaso[s]?[\!\?\,\.]?\b[\!\?\,\.]?\b"
        case "elote feliz":
            regex = r"\belote[s]?\sfeli[z]?[c]?[e]?[s]?[\!\?\,\.]?\b"
        case "elote desgranado":
            regex = r"\belote[s]?\sdesgranado[s]?[\!\?\,\.]?\b"
        case "coctel de elote":
            regex = r"\bc[oó]?[c]?[k]?[ck]?t[eé]?l[e]?[s]?[\s\w]\sde\selote[s]?[\!\?\,\.]?\b"
        case "queso Oaxaca":
            regex = r"\bqueso[s]?\s[Oo]?axaca[\!\?\,\.]?\b"
        case "queso de hebra":
            regex = r"\bqueso[s]?\s[d]?[e]?\shebra[\!\?\,\.]?\b"
        case 'asquiline':
            regex = r"\b[ea]?squilin[e]?[s]?[\!\?\,\.]?\b"
        case "mosquito":
            regex = r"\bmosquito[s]?[\!\?\,\.]?\b"
        case "chaquiste":
            regex = r"\bcha[n]?quiste[s]?[\!\?\,\.]?\b"
        case "colibri":
            regex = r"\bcolibr[ií]?[e]?[s]?[\!\?\,\.]?\b"
        case "chuparrosa":
            regex = r"\bchupa[r]?rosa[s]?[\!\?\,\.]?\b"
        case "automovil":
            regex = r"\bautom[oó]?vil[e]?[s]?[\!\?\,\.]?\b"
        case "habitacion":
            regex = r"(?!casa[s]?)\bhabitaci[oó]?n[e]?[s]?[\!\?\,\.]?\b"
        case "recamara":
            regex = r"\brec[aá]?mara[s]?[\!\?\,\.]?\b"
        case "comezon":
            regex = r"\bcomez[oó]?n[e]?[s]?[\!\?\,\.]?\b"
        case "picazon":
            regex = r"\bpicaz[oó]?n[e]?[s]?[\!\?\,\.]?\b"
        case "cinturon":
            regex = r"\bcintur[oó]?n[e]?[s]?[\!\?\,\.]?\b"
        case "escusado":
            regex = r"\be[sx]?cusado[s]?[\!\?\,\.]?\b"
        case "WC":
            regex = r"\bWC\b"
        case "brasier":
            regex = r"\bbras[s]?ier[e]?[s]?[\!\?\,\.]?\b"
        case "fajo":
            regex = r"\fajo[s]?[\!\?\,\.]?\s+(?!billete[s]?)"
        case _:
            regex = fr"\mosquito?[s]?[\!\?\,\.]?\b"

    query = {
        "$or" : [
            {"place": {"$ne": None}}, 
                {"geo": {"$ne": None}}, 
        ], 
        "text": {"$regex": regex,  "$options": "i"}
        }
    return query

def graceful_auto_reconnect(mongo_op_func):
    """Gracefully handle a reconnection event."""
    @functools.wraps(mongo_op_func)
    def wrapper(*args,  **kwargs):
        for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
            try:
                return mongo_op_func(*args,  **kwargs)
            except pymongo.errors.AutoReconnect as e:
                wait_t = 0.5 * pow(2,  attempt) # exponential back off
                logging.warning("PyMongo auto-reconnecting... %s. Waiting %.1f seconds.",  str(e),  wait_t)
                time.sleep(wait_t)

        raise Exception(f"Failed after {MAX_AUTO_RECONNECT_ATTEMPTS} attempts.")
    return wrapper

# Your MongoDB query function that you want to handle gracefully
@graceful_auto_reconnect
def perform_mongo_query(my_query):
        # Start a session
    with client.start_session() as session:
        # Set the session to be used for the following operations
        with session.start_transaction():# Start a session
            cursor = collection.find(my_query,  no_cursor_timeout=True)
    return cursor

# Specify the CSV file path
def write_csv_data(csv_file_path, data):
    with open(csv_file_path, "a", newline="", encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if csvfile.tell() == 0:
            # If the file is empty,  write the header row
            writer.writerow(['Tweet', 'Fecha', 'Locacion', 'Longitud(V1)', 'Latitud(V1)', 'Longitud(V2)', 'Latitud(V2)', 'Longitud(V3)', 'Latitud(V3)', 'Longitud(V4)', 'Latitud(V4)'])

        for tweet in data:
            placedata = tweet['place']
            data = [tweet['text'], tweet['created_at'], placedata['full_name']]
            vertices = placedata['bounding_box']['coordinates'][0]
            cordenadas = []
            for vertex in range(len(vertices)):
                cordenadas.extend([vertices[vertex][0], vertices[vertex][1]])
            data.extend(cordenadas)
            writer.writerow(data)
    print(f'Tweets saved to {csv_file_path}...')

# Execute the query and retrieve the matching tweets
def retrieve_tweets(args):
    for concept in args:
        print(concept)
        if any(concept in val for val in conceptos.values()):
            concepto = [key for key, value in conceptos.items() if concept in value][0]
            conceptdir=os.path.join(datadir,concepto)
            if not os.path.exists(conceptdir):
                os.makedirs(conceptdir)
                print(f"Making {conceptdir} directory...")
            start_time=time.time()
            query=make_query(concept)
            tweets = perform_mongo_query(query)
            filename = f"mongodb-{concept}.csv"
            csv_file = os.path.join(conceptdir, filename)
            if not os.path.isfile(csv_file):
                write_csv_data(csv_file, tweets)
                tweets.close()
                stop_time=time.time()
                time_delta=start_time-stop_time
                print(f"csv file {csv_file} writen.")
                print(f"---- Time needed: {time_delta} ----")
            else:
                print(f"csv file {csv_file} already writen skiping...")
        else:
            print(f"""Concept '{concept}' not in concept list.""")
            start_time=time.time()
            query=make_query(concept)
            tweets = perform_mongo_query(query)
            filename = f"mongodb-{concept}.csv"
            csv_file = os.path.join(datadir, filename)
            if not os.path.isfile(csv_file):
                write_csv_data(csv_file, tweets)
                tweets.close()
                stop_time=time.time()
                time_delta=start_time-stop_time
                print(f"csv file {csv_file} writen.")
                print(f"---- Time needed: {time_delta} ----")
            else:
                print(f"csv file {csv_file} already writen skiping...")

parser=argparse.ArgumentParser(description='Make database .csv file from the fetched database per word, this includes the geodata and tweets')
parser.add_argument('conceptos',metavar='concept',type=str,nargs='*')
parser.parse_args(['-'])
cl_argument=parser.parse_args()
concepts=cl_argument.conceptos
retrieve_tweets(concepts)

# Close the MongoDB connection
client.close()
