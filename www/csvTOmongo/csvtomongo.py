import csv
import time 
from pprint import pprint
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
	db = connection_mongo.connection_db(host=config.HOST_MONGO, port= config.PORT_MONGO, database = DB)
	meta = db['meta_csvtotab']

	for e in meta.find():
		collection_name = e['table_name']
		collection = db[collection_name]
		collection.drop()

	meta.drop()

def col_struct(nbr_column):
	
	struct = {"REFERENCE":""}
	cpt=1
	for i in range(nbr_column):
		struct["COL{}".format(i+1)]=""
	return struct


def update_data(nbr_column, data):
	all_data = []
	for element in data:
		struct = tab_struct(nbr_column)
		for key in struct :
			struct[key]= element[key] if key in element else ""
		all_data.append(struct)
	return all_data

def create_table(nbr_column, table_name, data): 
	db = connection_mongo.connection_db(host = config.HOST_MONGO,port = config.PORT_MONGO,database = DB)
	col_create = db[table_name]
	col_insert_in_meta = db['meta_csvtotab']
	col_insert_in_meta.insert_one({'table_name': table_name})
	

	for element in data:
		collection = col_struct(nbr_column)
		for key in collection:
			collection[key] = element[key] if key in element else ""
		col_create.insert_one(collection)

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

def show_mongo_db(db_name):
	db = connection_mongo.connection_db(host = config.HOST_MONGO, port = config.PORT_MONGO, database = db_name)

	for collection in db.list_collection_names():
		col = db[collection]
		print('\n=========',collection,'=========\n')
		for e in col.find():
			print('\n---')
			print(e)
		print('\n\n')

def main():
	started_time = time.time() 
	print('\n\n######  STARTED  ######')

	print('etape 1: préparation des données:')
	nbr_column, data = open_csvfile(path_file="../APP/file_uploaded/csvfile.csv",delimiter=";")
	#pprint(data)
	print('Recherche de double pure:')
	data = clean_pure_double(data)
	print('Recherche de guillemet')
	data = clean_quote(data)

	print('\n\nBD: Vidage de la BD précedente')
	clean_database()

	print('BD: Creation et insertion des donnée dans la BD')
	create_table(nbr_column, table_name, data)

	show_mongo_db(DB)


	print('\nFINISHED IN {} SECONDS\n'.format(round(time.time()-started_time,2)))

main()