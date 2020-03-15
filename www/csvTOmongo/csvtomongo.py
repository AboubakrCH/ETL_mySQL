import csv
import time 
from pprint import pprint
import connection_mongo
import config
from datetime import datetime
import re
from datetime import date
import json

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
	db, client = connection_mongo.connection_db(host=config.HOST_MONGO, port= config.PORT_MONGO, database = DB)

	meta = db['meta_csvtotab']

	for e in meta.find():
		collection_name = e['table_name']
		collection = db[collection_name]
		collection.drop()

	meta.drop()
	client.close()

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
	db,client = connection_mongo.connection_db(host = config.HOST_MONGO,port = config.PORT_MONGO,database = DB)

	col_create = db[table_name]
	col_insert_in_meta = db['meta_csvtotab']
	col_insert_in_meta.insert_one({'table_name': table_name})
	

	for element in data:
		collection = col_struct(nbr_column)
		for key in collection:
			collection[key] = element[key] if key in element else ""
		col_create.insert_one(collection)

	client.close()

def create_DR_CSVFILE_COL(nbr_column,table_name):
	db,client = connection_mongo.connection_db(host = config.HOST_MONGO,port = config.PORT_MONGO,database = DB)
	meta = db['meta_csvtotab']

	table = db[table_name]

	for i in range(nbr_column):
		meta.insert_one({'table_name' : 'DR_CSVFILE_COL_{}'.format(i+1)})

	for element in table.find():

		for i in range(nbr_column):
			col_i = db['DR_CSVFILE_COL_{}'.format(i+1)]

			OLDVALUES = element['COL{}'.format(i+1)]
			SYNTATICTYPE, SUBSYNTACTICTYPE = get_type(OLDVALUES)


			data = { 'REFERENCE': 'CSVFILE_{}_Col{}'.format(str(datetime.today().date()),i+1),
					'OLDVALUES' : '{}'.format(OLDVALUES),
					'SYNTATICTYPE' : '{}'.format(SYNTATICTYPE),
					'SUBSYNTACTICTYPE' : '{}'.format(SUBSYNTACTICTYPE),
					'COLUMNWIDTH' : len(OLDVALUES),	
					'NUMBEROFWORDS' : len(str(OLDVALUES).split(" "))
					}
			#print(data)

			col_i.insert_one(data)

	client.close()

def get_type(txt):
	db,client = connection_mongo.connection_db(host = config.HOST_MONGO,port = config.PORT_MONGO,database = DB)
	reg = db['regex']

	for element in reg.find():
		type_ = element['type']
		subtype = element['subtype']
		regex = element['regex']

		x = re.match(regex, txt)
		if x : 
			client.close()
			return type_,subtype

	client.close()
	return "UNKNOWN","UNKNOWN"

def get_type_dominant(tabnum):
	db,client = connection_mongo.connection_db(host = config.HOST_MONGO,port = config.PORT_MONGO,database = DB)
	col_i = db['DR_CSVFILE_COL_{}'.format(tabnum)]

	query = [{ '$group' : {'_id' : '$SUBSYNTACTICTYPE', 'count': {'$sum' : 1}}},
			 {'$match' : {'SUBSYNTACTICTYPE' : {'$ne' : 'UNKNOWN' }} }]

	res = col_i.aggregate(query)

	dominant = ''
	max = -1
	for element in res:
		if element['count'] > max:
			max = element['count']
			dominant = element['_id']

	return dominant

def detect_anomaly(num):
	db,client = connection_mongo.connection_db(host = config.HOST_MONGO,port = config.PORT_MONGO,database = DB)
	col = db['DR_CSVFILE_COL_{}'.format(num)]

	dominant = get_type_dominant(num)

	for element in col.find():
		if element['SUBSYNTACTICTYPE'] != dominant:
			id = { '_id' : element['_id'] }
			update = { '$set' : {'OBSERVATION' : "{}<?>Anomaly_WrongType".format(element['OLDVALUES'])}}
			col.update_one(id,update)
		elif element['SUBSYNTACTICTYPE'] == "":
			id = { '_id' : element['_id'] }
			update = { '$set' : {'OBSERVATION' : "NULL<?>Anomaly"}}
			col.update_one(id,update)
		else:
			id = { '_id' : element['_id'] }
			update = { '$set' : {'OBSERVATION' : ""}}
			col.update_one(id,update)

	client.close()	

def new_val(tabnum):
	db,client = connection_mongo.connection_db(host = config.HOST_MONGO,port = config.PORT_MONGO,database = DB)
	col = db['DR_CSVFILE_COL_{}'.format(tabnum)]

	for element in col.find():
		if element['OBSERVATION'] == "":
			id = { '_id' : element['_id'] }
			update = { '$set' : {'NEWVALUES' : "{}".format(element['OLDVALUES'])}}
			col.update_one(id,update)
		else:
			id = { '_id' : element['_id'] }
			update = { '$set' : {'NEWVALUES' : "{}".format(element['OBSERVATION'])}}
			col.update_one(id,update)

	client.close()	

def check_semantique(tabnum):
	db,client = connection_mongo.connection_db(host = config.HOST_MONGO,port = config.PORT_MONGO,database = DB)
	col = db['DR_CSVFILE_COL_{}'.format(tabnum)]
	reg_lst = db['ddre']
	ddvs_lst = db['ddvs']

	for data in col.find():
		if data['OLDVALUES'] != "":
			found = False

			for reg in reg_lst.find():

				if re.match(reg['regex'].upper(),data['OLDVALUES'].upper()):
					id = { '_id' : data['_id'] }
					update = { '$set' : {'SEMANTICCATEGORY' : "{}".format(reg['cat']), 'SEMANTICSUBCATEGORY': "{}".format(reg['subcat'])}}
					col.update_one(id,update) 

					found = True

			if not found :
				id = { '_id' : data['_id'] }
				update = { '$set' : {'SEMANTICCATEGORY' : "UNKNOWN", 'SEMANTICSUBCATEGORY': "UNKNOWN"}}
				col.update_one(id,update) 
		else:
			id = { '_id' : data['_id'] }
			update = { '$set' : {'SEMANTICCATEGORY' : "NULL", 'SEMANTICSUBCATEGORY': "NULL"}}
			col.update_one(id,update)



	client.close()				


def update_DR_CSVFILE_COL(nbr_column):

	for i in range(nbr_column):
		detect_anomaly(i+1)
		new_val(i+1)
		check_semantique(i+1)

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

def show_mongo_collections(db_name):
	db, client = connection_mongo.connection_db(host = config.HOST_MONGO, port = config.PORT_MONGO, database = db_name)

	for collection in db.list_collection_names():
		col = db[collection]
		print('\n=========',collection,'=========\n')
		for e in col.find():
			print('\n---')
			print(e)
		print('\n\n')

	client.close()

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


	
	print('\n\nprofiling: création des tables par colonne')
	create_DR_CSVFILE_COL(nbr_column,table_name)

	print('profiling: analyse des colonnes')
	update_DR_CSVFILE_COL(nbr_column)
	


	print('\nFINISHED IN {} SECONDS\n'.format(round(time.time()-started_time,2)))

main()