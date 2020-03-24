import csv
import time 
from pprint import pprint
import connection_mongo
from datetime import datetime
import re
from datetime import date
import json
import threading
import config
from REGEXPRES import update_regex

DB = "csvtotab"
table_name = "CSV2TABCOLUMNS"
db,client = connection_mongo.connection_db(host = config.HOST_MONGO,port = config.PORT_MONGO,database = DB)

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
	col_create = db[table_name]
	col_insert_in_meta = db['meta_csvtotab']
	col_insert_in_meta.insert_one({'table_name': table_name})
	

	for element in data:
		collection = col_struct(nbr_column)
		for key in collection:
			collection[key] = element[key] if key in element else ""
		col_create.insert_one(collection)

	


#Threading the creation of the dr csv file col i
def th_drcsvfile2(i,element):
	
	col_i = db['DR_CSVFILE_COL_{}'.format(i+1)]

	OLDVALUES = element['COL{}'.format(i+1)]
	SYNTACTICTYPE, SUBSYNTACTICTYPE = get_type(OLDVALUES)


	data = { 'REFERENCE': 'CSVFILE_{}_Col{}'.format(str(datetime.today().date()),i+1),
			'OLDVALUES' : '{}'.format(OLDVALUES),
			'SYNTACTICTYPE' : '{}'.format(SYNTACTICTYPE),
			'SUBSYNTACTICTYPE' : '{}'.format(SUBSYNTACTICTYPE),
			'COLUMNWIDTH' : len(OLDVALUES),	
			'NUMBEROFWORDS' : len(str(OLDVALUES).split(" "))
		}
	#print(data)

	col_i.insert_one(data)	

'''
#Testing Nested execution of threads, works but need to study how effective it is, and when to use it.
def th_drcsvfile1(nbr_column,element):
	threads = list()
	for i in range(nbr_column):
		x = threading.Thread(target = th_drcsvfile2,args = (i,element))
		x.start()
		threads.append(x)
		#print("start ",i)

	for t in threads:
		#print("waiting for ",index)
		t.join()
'''
def create_DR_CSVFILE_COL(nbr_column,table_name):
	meta = db['meta_csvtotab']

	table = db[table_name]

	for i in range(nbr_column):
		meta.insert_one({'table_name' : 'DR_CSVFILE_COL_{}'.format(i+1)})

	#ths = list()
	for element in table.find():

		#x = threading.Thread(target = th_drcsvfile1, args = (nbr_column,element))
		#x.start()
		#ths.append(x)

		
		threads = list()
		for i in range(nbr_column):
			x = threading.Thread(target = th_drcsvfile2,args = (i,element))
			x.start()
			threads.append(x)
			#print("start ",i)

		for t in threads:
			#print("waiting for ",index)
			t.join()
		'''
	for t in ths:
		t.join()
		'''
	

def get_type(txt):
	reg = db['regex']

	for element in reg.find():
		type_ = element['type']
		subtype = element['subtype']
		regex = element['regex']

		x = re.match(regex, txt)
		if x :
			
			return type_,subtype

	
	return "UNKNOWN","UNKNOWN"

def get_type_dominant(tabnum):
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

def get_semantic_dominant(tabnum):
	col_i = db['DR_CSVFILE_COL_{}'.format(tabnum)]

	query = [{ '$group' : {'_id' : '$SEMANTICCATEGORY', 'count': {'$sum' : 1}}},
			 {'$match' : {'SEMANTICCATEGORY' : {'$ne' : 'NULL' }} }]

	res = col_i.aggregate(query)

	dominant = ''
	max = -1
	for element in res:
		if element['count'] > max:
			max = element['count']
			dominant = element['_id']

	
	return dominant

def get_subsemantic_dominant(tabnum):
	col_i = db['DR_CSVFILE_COL_{}'.format(tabnum)]

	query = [{ '$group' : {'_id' : '$SEMANTICSUBCATEGORY', 'count': {'$sum' : 1}}},
			 {'$match' : {'SEMANTICSUBCATEGORY' : {'$ne' : 'NULL' }} }]

	res = col_i.aggregate(query)

	dominant = ''
	max = -1
	for element in res:
		if element['count'] > max:
			max = element['count']
			dominant = element['_id']

	
	return dominant

def detect_anomaly(num):
	col = db['DR_CSVFILE_COL_{}'.format(num)]

	dominant = get_type_dominant(num)

	for element in col.find():
		if element['OLDVALUES'] == "" or element['OLDVALUES'].upper() == "NULL":
			id = { '_id' : element['_id'] }
			update = { '$set' : {'OBSERVATION' : "NULL<?>Anomaly"}}
			col.update_one(id,update)
		elif element['SUBSYNTACTICTYPE'] != dominant:
			id = { '_id' : element['_id'] }
			update = { '$set' : {'OBSERVATION' : "{}<?>Anomaly_WrongType".format(element['OLDVALUES'])}}
			col.update_one(id,update)
		else:
			id = { '_id' : element['_id'] }
			update = { '$set' : {'OBSERVATION' : ""}}
			col.update_one(id,update)

		

def new_val(tabnum):
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

		

def check_semantique(tabnum):
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



					


def update_DR_CSVFILE_COL(nbr_column):

	threads = list()
	for i in range(nbr_column):
		x = threading.Thread(target = detect_anomaly, args = (i+1,))
		x.start()
		threads.append(x)

	for t in threads:
		t.join()


	tnv = list()
	tcs = list()
	for i in range(nbr_column):
		x = threading.Thread(target = new_val, args = (i+1,))
		x.start()
		tnv.append(x)

		y = threading.Thread(target = check_semantique, args = (i+1,))
		y.start()
		tcs.append(y)

	for t in tnv:
		t.join()

	for t in tcs:
		t.join()

def clean_pure_double(data):

	noneDupData = []
	noneDupData.append(data[0]) #We put the first element of the data in the list

	#For each element in the data we check if the element is already in the list we don"t re-add it, 
	#else we add it in the list and we proceed with the next element of the data
	for x in data:
		dup= False #Variable to check if the element already exist
		for y in noneDupData:
			if x == y:
				dup = True
				break

		if dup == False:
			noneDupData.append(x)
		

	return noneDupData

def fetch_m(num, db_name):
	col = db['DR_CSVFILE_COL_{}'.format(num)]

	M000 = col.count_documents({})
	M100 = col.count_documents({'OBSERVATION' : "NULL<?>Anomaly"})
	#M101 = col.count_documents({'OBSERVATION' : {'$ne':"NULL<?>Anomaly"}})
	M101 = M000 - M100

	tmp = col.find({},{'COLUMNWIDTH' : 1, '_id': 0})


	M102 = tmp.sort('COLUMNWIDTH' , 1).limit(1)
	M103 = tmp.sort('COLUMNWIDTH' ,-1).limit(1)

	M102 = M102[0]['COLUMNWIDTH']
	M103 = M103[0]['COLUMNWIDTH']

	M104 = 0

	tmp = col.find({},{'NUMBEROFWORDS' : 1, '_id': 0}).sort('NUMBEROFWORDS' , -1).limit(1)

	if tmp[0]['NUMBEROFWORDS'] > M104 and col.count_documents({'NUMBEROFWORDS' : {'$gt': 0}}) > 0:
		M104 = tmp[0]['NUMBEROFWORDS']

	#tmp = col.find({},{'SYNTACTICTYPE' : 1, '_id':0})
	#replace this part with an automated count of the syntactic type#
	list = []
	M105 = col.count_documents({'SYNTACTICTYPE' : 'VARCHAR'})
	list.append([M105,"VARCHAR"])
	M106 = col.count_documents({'SYNTACTICTYPE' : 'NUMBER'})
	list.append([M106,"NUMBER"])
	M107 = col.count_documents({'SYNTACTICTYPE' : 'DATE'})
	list.append([M107,"DATE"])
	M108 = col.count_documents({'SYNTACTICTYPE' : 'BOOLEAN'})
	list.append([M108,"BOOLEAN"])
	M109 = col.count_documents({'SYNTACTICTYPE' : 'NULL'})
	#list.append(M109) We don't want a null type column


	M110 = len(col.distinct('OLDVALUES'))

	M111 = "VARCHAR"
	max = M105
	for x in list:
		if max < x[0]:
			max = x[0]
			M111 = x[1]


	M112 = col.count_documents({'OBSERVATION' : {'$ne' : ''}})
	M113 = M000 - M112

	M114 = get_semantic_dominant(num)

	M115 = col.count_documents({'SEMANTICCATEGORY' : {'$ne' : M114}})
	M116 = col.count_documents({'SEMANTICCATEGORY' : {'$eq' : M114}})

	M117 = get_subsemantic_dominant(num)

	M118 = col.count_documents({'SEMANTICSUBCATEGORY' : {'$ne' : M117}})
	M119 = col.count_documents({'SEMANTICSUBCATEGORY' : {'$eq' : M117}})



	data = {
			"order" : num,
			"REFERENCE" : "CSV_FILE_{}".format(date.today()),
			"Old Name" : "COL{}".format(num),
			"New Name" : "COL{}_{}".format(num,M117),
			"Nbr of values" : M000,
			"Nbr of not null values" : M100,
			"Nbr of null values" : M101,
			"Max column width" : M102,
			"Min column width" : M103,
			"Max number of word" : M104,
			"Nbr of VARCHAR" : M105,
			"Nbr of NUMBER" : M106,
			"Nbr of DATE" : M107,
			"Nbr of BOOLEAN" : M108,
			"Nbr of NULL" : M109,
			"Nbr of distinct value" : M110,
			"Dominant Sytactic type" : M111,
			"Nbr of empty OBSERVATION" : M112,
			"Nbr of OBSERVATION" : M113,
			"Dominant semantic" : M114,
			"Nbr of ne semantic" : M115,
			"Nbr of eq semantic" : M116,
			"Dominant sub semantic" : M117,
			"Nbr of ne sub semantic" : M118,
			"Nbr of eq sub semantic" : M119
			}


	col = db['DR_CSVFILE_TabCol']
	col.insert_one(data)
	

def create_m0(nbr_column, db_name):
	meta = db['meta_csvtotab']
	meta.insert_one({'table_name' : 'DR_CSVFILE_TabCol'})


	threads = list()
	for i in range(nbr_column):
		x = threading.Thread(target = fetch_m, args = (i+1,db_name,))
		x.start()
		threads.append(x)

	for t in threads:
		t.join()

	


def clean_quote(data):

	corrected_data = []

	for x in data:
		for key in x:
			x[key] = x[key].replace("''","'") # we first replace all '' by a single ' as it in most case it's an error
			x[key] = x[key].replace('"',"''") # here we replace " by '' as it will create error in constructed querries
		corrected_data.append(x)

	return corrected_data

def show_mongo_collections(db_name):

	for collection in db.list_collection_names():
		col = db[collection]
		print('\n=========',collection,'=========\n')
		for e in col.find():
			print('\n---')
			print(e)
		print('\n\n')


#-------------Table final ------------
def gatherlign(num,l):

	col = db['DR_CSVFILE_COL_{}'.format(num+1)]
	result = col.find({},{'NEWVALUES':1,'OLDVALUES':1})
	for e in result:
		#print(e['NEWVALUES'])
		l[num] = l[num] + [e['NEWVALUES']]



def rebuild_csv_table(nbrcol, table_name):
	liste = list()


	threads = list()

	for i in range(nbrcol):
		x = list()
		liste.append(x)

	for i in range(nbrcol):
		x = threading.Thread(target = gatherlign, args = (i,liste))
		x.start()
		threads.append(x)

	for t in threads:
		t.join()

	format = matchwithnames()
	#print(format)

	meta = db['meta_csvtotab']
	meta.insert_one({'table_name': table_name})

	threads = list()
	col = db[table_name]

	#print(liste)
	for index, e in enumerate(liste[0]):
		lign = list()
		for i in range(nbrcol):
			try:
				lign.append(liste[i][index])
			except:
				lign.append('') #for some reason, the number of column is some time wrong will investigate later
		x = threading.Thread(target = insert_lign, args =(lign,format,col))
		x.start()
		threads.append(x)

	for t in threads:
		t.join()	


def insert_lign(lign,format,col):
	data = {}
	for key in format:
		for value in lign:
			data[key] = value
			lign.remove(value)
			break

	#print(data)
	col.insert_one(data)
	#pprint(tmp)



def matchwithnames():
	col = db['DR_CSVFILE_TabCol']
	liste = list()
	result = col.find({},{"New Name" : 1, "order" : 1}).sort([("order",1)])
	for e in result:
		#pprint(e)
		#print(e['New Name'])
		liste.append(e['New Name'])
	
	return liste

#-----------MAIN------------

def main():
	started_time = time.time() 
	print('\n\n######  STARTED  ######')

	print('Initialisation des tables regex')
	update_regex('mongodb')

	#'''
	print('etape 1: préparation des données:')
	nbr_column, data = open_csvfile(path_file="../APP/file_uploaded/csvfile.csv",delimiter=",")
	#pprint(data)
	#print(nbr_column)
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
	
	print('profiling: profiling des données')
	create_m0(nbr_column,DB)

	print('\nFinalisation: création de la table final')
	rebuild_csv_table(6,"NEWCSV2TABCOLUMNS")

	'''
	col = db["NEWCSV2TABCOLUMNS"]
	cursor = col.find()

	for e in cursor:
		print(e)

	print('^table final with {} ligne^'.format(col.count_documents({})))
	
	#test part
	db,client = connection_mongo.connection_db(host = config.HOST_MONGO, port = config.PORT_MONGO, database = DB)
	col = db['DR_CSVFILE_COL_{}'.format(4)]
	x = len(col.distinct('OLDVALUES'))
	#x = x.sort('COLUMNWIDTH', 1).limit(1)
	pprint(col.count_documents({'NUMBEROFWORDS' : {'$gt' : 0}}))
	show_mongo_collections(DB)
	#for e in x:
	#	pprint(e)

	
	#'''
	client.close()
	print('\nFINISHED IN {} SECONDS\n'.format(round(time.time()-started_time,2)))

main()