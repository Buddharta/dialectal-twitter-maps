#!/usr/bin/python3
import logging
import datetime
import time
from pymongo import MongoClient
from urllib.parse import quote_plus
import csv
import os
import pymongo
import logging
import time
import functools
import argparse
from dotenv import load_dotenv

load_dotenv()

MONGO_DB = os.getenv('DB')
MONGO_HOST = os.getenv('DB_HOST')
MONGO_USER = os.getenv('DB_USER')
MONGO_PASS = os.getenv('PASS')
MONGO_MECHANISM = os.getenv('MECHANISM')
MAX_AUTO_RECONNECT_ATTEMPTS = 5

uri = f'mongodb://{quote_plus(MONGO_USER)}:{quote_plus(MONGO_PASS)}@{MONGO_HOST}/{MONGO_DB}'
client = MongoClient(uri, socketTimeoutMS=90000000)
db = client.twitterdb
collection = db.tweetsMexico

MAX_AUTO_RECONNECT_ATTEMPTS = 5
HOME = os.environ["HOME"]
workdir=os.path.join(HOME,"repos/dialectic-twitter-maps-generator")
datadir=os.path.join(workdir,'data')
#conceptdir=os.path.join(datadir,concept)

conceptos={
    'esquite':['esquite', 'trolelote', 'chasca', 'elote en vaso', 'vasolote', 'elote feliz', 'coctel de elote', 'elote desgranado'], 
    'bolillo':['bolillo', 'birote'], 
    'migaja':['migaja', 'borona', 'morona', 'morusa'], 
    'queso Oaxaca':['queso Oaxaca', 'quesillo', 'queso de hebra'], 
    'hormiga':['hormiga', 'asquel', 'asquilin'], 
    'mosquito':['mosquito','zancudo','chaquiste','chanquiste','moyote'],
    'pavo':['pavo', 'guajolote', 'totole', 'totol', 'chompipe'], 
    'colibri':['colibri', 'chupamirto', 'chuparrosa', 'chupaflor'], 
    'automovil':['coche', 'automovil', 'carro', 'auto'], 
    'aguacero':['aguacero', 'chubasco', 'tormenta'], 
    'habitacion':['habitacion', 'alcoba', 'dormitorio', 'recamara'], 
    'cobija':['cobija', 'frazada'], 
    'lentes':['lentes', 'anteojo', 'gafas', 'espejuelos'], 
    'rasguño':['rasguño', 'arañazo'], 
    'lagaña':['legaña', 'lagaña', 'chinguiña'], 
    'comezon':['comezon', 'picazon', 'rasquera', 'rasquiña'],  
    'cinturon':['cinturon', 'cinto', 'fajo'],  #(bucar fajo con opción “sin billetes” en la expresión regular)
    'escusado':['retrete', 'escusado', 'inodoro', 'WC'],
    'brasier':['brasier', 'chichero']  
}

class query:
    def __init__(self, term :str):
        self.term = term
        match self.term:
            case "chasca":
                self.regex = r"\bchas[ck]?a[s]?[\!\?\,\.]?\b"
            case "elote en vaso":
                self.regex = r"\belote[s]?[\s\w] en vaso[s]?[\!\?]?[\s\w]\b"
            case "elote feliz":
                self.regex = r"\belote[s]?[\s\w] feli[z]?[c]?[e]?[s]?[\!\?\,\.]?\b"
            case "elote desgranado":
                self.regex = r"\belote[s]?[\s\w] desgranado[s]?[\!\?\,\.]?\b"
            case "coctel de elote":
                self.regex = r"\bcoctel[e]?[s]?[\s\w] de elote[s]?[\!\?\,\.]?\b"
            case "queso Oaxaca":
                self.regex = r"\bqueso[s]?[\s\w] [Oo]?axaca[\!\?\,\.]?\b"
            case "queso de hebra":
                self.regex = r"\bqueso[s]?[\s\w] [d]?[e]? hebra[\!\?\,\.]?\b"
            case 'asquiline':
                self.regex = r"\b[ea]?squilin[e]?[s]?[\!\?\,\.]?\b"
            case "chaquiste":
                self.regex = r"\bcha[n]?quiste[s]?[\!\?\,\.]?\b"
            case "colibri":
                self.regex = r"\bcolibr[ií]?[e]?[s]?[\!\?\,\.]?\b"
            case "automovil":
                self.regex = r"\bautom[oó]?vil[e]?[s]?[\!\?\,\.]?\b"
            case "habitacion":
                self.regex = r"\bhabitaci[oó]?n[e]?[s]?[\!\?\,\.]?\b"
            case "recamara":
                self.regex = r"\brec[aá]?mara[s]?[\!\?\,\.]?\b"
            case "comezon":
                self.regex = r"\bcomez[oó]?n[e]?[s]?[\!\?\,\.]?\b"
            case "picazon":
                self.regex = r"\bpicaz[oó]?n[e]?[s]?[\!\?\,\.]?\b"
            case "cinturon":
                self.regex = r"(de seguridad)\bcintur[oó]?n[e]?[s]?[\!\?\,\.]?\b"
            case "escusado":
                self.regex = r"\be[sx]?cusado[s]?[\!\?\,\.]?\b"
            case "WC":
                self.regex = r"\bWC\b"
            case "brasier":
                self.regex = r"\bbras[s]?ier[e]?[s]?[\!\?\,\.]?\b"
            case "fajo":
                self.regex = r"(?!billete[s]?)\bfajo[s]?[\!\?\,\.]?\b"
            case _:
                self.regex = fr"\b{term}[e]?[s]?[\!\?\,\.]?\b"


    def query(self):
        query = {
            "$or" : [
                {"place": {"$ne": None}},
                    {"geo": {"$ne": None}},
            ],
            "text": {"$regex": self.regex,  "$options": "i"}
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

parser=argparse.ArgumentParser(description='Make database .csv file from the fetched database per word, this includes the geodata and tweets')
parser.add_argument('concepto',metavar='concept',type=str)
parser.parse_args(['-'])
cl_argument=parser.parse_args()
concept=cl_argument.concepto
conceptdir=os.path.join(datadir,concept)

# Execute the query and retrieve the matching tweets

last_query_time = 0
while True:
    if (concept not in conceptos.keys() or concept not in coceptos.values()):
        logging.warning(f"Concept {concept} is not in concept list!")
    q=query(concept)
    tweets = perform_mongo_query(q.query())
    now = datetime.datetime.now()
    last_query_time = now
    for data in tweets:
        output = {
            "user": {
                "name": data["user"]["name"],"screen_name": data["user"]["screen_name"], "id":data["user"]["id_str"]
                },
            "tweet":{
                "tweet_id": data['id_str'],"text": data['text'] ,"created_at": data['created_at'], "url": data['source']
            },
            "place": data['place'],
            "last_query_time": now
        }
        print(f"Termino buscado: {q.term:<10}, regex: {q.regex:<25}")
        print("__________________________________________________________________________")
        print(f'''Usuario: {output["user"]["name"]:<10}, id: {output["user"]["id"]:<10}''')
        print("__________________________________________________________________________")
        print(f'''Tweet: {output["tweet"]["text"]:<240}''')
        print(f'''Tweet_ID: {output["tweet"]["tweet_id"]:<240}''')
        print("__________________________________________________________________________")
        time.sleep(2)
#f = io.StringIO()
#with redirect_stdout(f):
#    fix_places(file_dir,fname)
#out = f.getvalue()
# Close the MongoDB connection
client.close()
