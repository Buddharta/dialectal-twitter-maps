#!/usr/bin/env python3
import csv
import os
import argparse

HOME = os.environ["HOME"]
#WD = os.path.join(HOME,"source/PYTHON/dilectal-twitter-maps")
WD = os.environ["PWD"]
datadir = os.path.join(WD,"data")

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
    'cinturon':['cinturon', 'cinto', 'fajo'],  
    'escusado':['retrete', 'escusado', 'inodoro', 'WC'], 
    'brasier':['brasier', 'chichero']  
}

# Function that removes the row in a csv file that contains a given string
def clean_csv_rows_with(remove_strings:list, inpath, outpath):
    with open(inpath,'r') as infile, open(outpath,'w') as outfile:
        csv_infile=csv.reader(infile)
        csv_oufile=csv.writer(outfile)
        for row in csv_infile:
            if not any(remove_string in element for element in row for remove_string in remove_strings):
                csv_oufile.writerow(row)
        print(f"New file saved in {outpath}")

# Maybe I should just make the above function case insensitive
def make_upper_lower_combinations(string:str) -> list[str]:
    list_of_combinations=[]
    return list_of_combinations
    
# parse input form command line standard in 
parser = argparse.ArgumentParser(description='Remove lines from a .csv file that contains a given term.')
parser.add_argument('file', type=argparse.FileType('r'))

#parser.add_argument('term', type=str)
parser.parse_args()
arguments = parser.parse_args()
open_file=arguments.file
#term=arguments.term

file_path=vars(arguments)['file'].name
fname=os.path.basename(file_path)
new_name=f"{fname[:-4]}-removed-terms.csv"
file_dir=os.path.dirname(file_path)
new_file_path=os.path.join(file_dir,new_name)
print(f"File path: {file_dir}")
print(f"Processing {fname}...")

term=''
remove_words=[]
print("Enter the terms to be removed:")
while term != ":end":
    term = input()
    remove_words.append(term)
    print("Rows with the following are going to be removed:")
    print(", ".join(remove_words))
    
clean_csv_rows_with(remove_words,file_path,new_file_path)
