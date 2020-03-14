import csv
import time 
import pprint
import connection_mongo
import config
from datetime import datetime
import re
from datetime import date

DB = "csvtotab"
table_name = "CSV2TABCOLUMNS"
def open_csvfile(path_file, delimiter):
	data = []
	max_column=0
	with open(path_file, encoding = 'utf8') as csv_file:
		print("Reading with encoding of utf8")
		csv_reader = csv.reader(csv_file, delimiter=delimiter)
		for row in csv_reader:
			line = {}
			line = {"REFERENCE":"CSVFILE_{}".format('__'.join(row))}
			#line = {"REFERENCE":"CSVFILE_"}
			i = 1
			for v in row : 
				line["COL{}".format(i)]= v
				i+=1
			data.append(line)
			if len(row) > max_column : 
				max_column = len(row)
	return max_column, data

def clean_database():
	client = connection_mongo.connection_db(host=config.HOST_MONGO, port= config.PORT_MONGO)
	db = client[DB]
	meta = db['meta_csvtotab']

	for e in meta:
		collection_name = e['table_name']
		collection = db[collection_name]
		collection.drop()



def clean_pure_double(data):

	noneDupData = []
	noneDupData.append(data[0]) #We  pute the first element of the data in the list

	#For each element in the data we check if the element is already in the list we don"t re-add it, 
	#else we add it in the list and we proceed with the next element of the data
	for x in data:
		dup= False #Variable to check if the element already exist
		for y in noneDupData:
			if x == y:
				dup = True

		if dup == False:
			noneDupData.append(x)
		

	return noneDupData

def clean_quote(data):

	corrected_data = []

	for x in data:
		for key in x:
			x[key] = x[key].replace("''","'") # we first replace all '' by a single ' as it in most case it's an error
			x[key] = x[key].replace('"',"''") # here we replace " by '' as it will create error in constructed querries
		corrected_data.append(x)

	return corrected_data

def main():
	started_time = time.time() 
	print('\n\n######  STARTED  ######')

	print('etape 1: préparation des données:')
	nbr_column, data = open_csvfile(path_file="../APP/file_uploaded/csvfile.csv",delimiter=";")
	pprint.pprint(data)
	print('Recherche de double pure:')
	data = clean_pure_double(data)
	print('Recherche de guillemet')
	data = clean_quote(data)

	print('\nFINISHED IN {} SECONDS\n'.format(round(time.time()-started_time,2)))

main()