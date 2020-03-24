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

DB = "csvtotab"
table_name = "CSV2TABCOLUMNS"
db,client = connection_mongo.connection_db(host = config.HOST_MONGO,port = config.PORT_MONGO,database = DB)


def gatherlign(num,l):

	col = db['DR_CSVFILE_COL_{}'.format(num+1)]
	result = col.find()
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

	for index, e in enumerate(liste[0]):
		lign = list()
		for i in range(nbrcol):
			lign.append(liste[i][index])
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



