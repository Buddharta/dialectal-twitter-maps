#!/usr/bin/env python
import csv
import os
import argparse
import pandas as pd

corr_estados={"Coahuila de Zaragoza":"Coahuila", "México" : "Estado de México", "Michoacán de Ocampo" : "Michoacán", "Veracruz de Ignacio de la Llave":"Veracruz"}
estados=["Aguascalientes", "Baja California", "Baja California Sur", "Campeche", "Chiapas", "Chihuahua", "Ciudad de México", "Coahuila de Zaragoza", "Colima", "Durango", "México", "Guanajuato", "Guerrero", "Hidalgo", "Jalisco", "Michoacán de Ocampo", "Morelos", "Nayarit", "Nuevo León", "Oaxaca", "Puebla", "Querétaro", "Quintana Roo", "San Luis Potosí", "Sinaloa", "Sonora", "Tabasco", "Tamaulipas", "Tlaxcala", "Veracruz de Ignacio de la Llave", "Yucatán", "Zacatecas"]


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
    'cinturón':['cinturon', 'cinto', 'fajo'],  
    'escusado':['retrete', 'escusado', 'inodoro', 'WC'], 
    'brasier':['brasier', 'chichero']  
}

HOME=os.environ["HOME"]
WD=os.environ["PWD"]
DATA_DIR=os.path.join(WD,"data")
OUT_DIR=os.path.join(WD,"outputs")
print(OUT_DIR)
def dictsum(dic2: dict, dic1 :dict) -> dict:
    return {key: dic1.get(key, 0) + dic2.get(key, 0) for key in set(dic1) | set(dic2)}

def get_location_data(file) -> dict:
    df = pd.read_csv(file,dtype={"Estado": str,"Poblacion": str,})
    apariciones=dict.fromkeys(estados,0)
    total=len(list(df["Estado"]))
    apariciones['Total']=total
    for item in df["Estado"]:
        apariciones[item]+=1
    return apariciones

def create_csv_files(concept :str):
    for palabra in conceptos[concept]:
        concept_fname=f"{palabra}-map.csv"
        concept_file=os.path.join(OUT_DIR,concept_fname)
        with open(concept_file,"a+", newline="", encoding='utf-8') as wordfile:
            if wordfile.tell() == 0:
                dbfile_path=os.path.join(DATA_DIR,f"{concept}/db-{palabra}-fixed.csv")
                print(f"Writing {concept_fname}.csv file...")
                data_by_concept=get_location_data(dbfile_path)
                writer = csv.writer(wordfile)
                writer.writerow(['Estado','Ocurrencias'])
                for value in data_by_concept.items():
                    if value[0] in corr_estados.keys():
                        correctos=[corr_estados[value[0]],value[1]]
                        writer.writerow(correctos)
                    else:
                        writer.writerow(list(value))
            else:
                print(f"File {concept_file} already written, skiping...")
                pass

    new_file_name=f"{concept}.csv"
    new_csv_file_path=os.path.join(OUT_DIR, new_file_name)
    
    with open(new_csv_file_path, "a+", newline="", encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        if outfile.tell() == 0: # If the file is empty,  write the header row
            write_data=[]
            header=["Estado"]
            header.extend(conceptos[concept])
            header.extend(list(map(lambda string : string + "_FR", conceptos[concept])))
            writer.writerow(header)
            for palabra in conceptos[concept]:
                file_path=os.path.join(DATA_DIR,f"{concept}/db-{palabra}-fixed.csv")
                print(f"Processing {file_path} data...")
                data_by_concept=get_location_data(file_path)
                write_data.append(data_by_concept)
            for estado in estados:
                if estado in corr_estados.keys():
                    row_data=[corr_estados[estado]]
                else:
                    row_data=[estado]
                ocurrence_data=[int(d[estado]) for d in write_data]
                total_sum=int(sum(ocurrence_data))
                relative_frequencies=list(map(lambda n : str(100.0*(n/total_sum)) ,ocurrence_data))
                row_tail=list((map(lambda n : str(n), ocurrence_data)))
                row_tail.extend(relative_frequencies)
                row_data.extend(row_tail)
                writer.writerow(row_data)
            final=['Total']
            totales=[d['Total'] for d in write_data]
            final.extend(totales)
            writer.writerow(final)
        else:
            print(f"File {concept}.csv already written, skipping..")

gabmap=True
parser=argparse.ArgumentParser(description='Make the conceptos.csv file from the fetched database files')
parser.add_argument('concepto',metavar='concept',type=str)
parser.add_argument('--gabmap',dest='gabmap',action='store_const',
                    const=gabmap, default=False,
                    help='Make the corresponding file to be processed by gabmap')
parser.parse_args(['-'])
cl_argument=parser.parse_args()
concept=cl_argument.concepto
gabmap_file_data=os.path.join(OUT_DIR,f'outputs/{concept}-gabmapdata.csv')
if (concept in conceptos):
    create_csv_files(concept)
else:
    print(f"""Sorry concept '{concept}' not in list.""")

##def get_places():
##    places=set()
##    for file in datafiles:
##        filepath=os.path.join(datadir,file)
##        with open(filepath,'r') as fl:
##            csv_reader=csv.DictReader(fl)
##            for row in csv_reader:
##                places.add(row['Locacion'])
##    return list(places)
##lugares=get_places()
##
##
##with open(outfile, "w", newline="",encoding='utf-8') as csvfile:
##    writer = csv.writer(csvfile)
##    if csvfile.tell() == 0:
##        # If the file is empty, write the header row
##        keys=['Palabra','Total de ocurrencias']
##        keys.extend(lugares)
##        writer.writerow(keys)
##
##    for file in datafiles:
##        ocurrencias={lugar:0 for lugar in lugares}
##        palabra=file[0:-4]
##        filepath=os.path.join(datadir,file)
##        with open(filepath,'r') as fl:
##            csv_reader=csv.DictReader(fl)
##            for row in csv_reader:
##                ocurrencias[row['Locacion']]+=1
##
##            valores=list(ocurrencias.values())
##            totales=sum(valores)
##            data=[palabra,totales]
##            data.extend(valores)
##            writer.writerow(data)
##
##
##with open(outdata, "w", newline="",encoding='utf-8') as csvfile:
##    writer = csv.writer(csvfile)
##    if csvfile.tell() == 0:
##        # If the file is empty, write the header row
##        keys=['Locacion','Retrete']
##        writer.writerow(keys)
##
##    with open(outfile,'r') as file:
##        csv_reader=csv.DictReader(file)
##        variantes={lugar:'' for lugar in lugares}
##        for row in csv_reader:
##            for lugar in lugares:
##                if int(row[lugar]) > 0:
##                    variantes[lugar]+=row['Palabra'] + " / "
##        for lugar in lugares:
##            data=[lugar,variantes[lugar][:-3]]
##            writer.writerow(data)
##

