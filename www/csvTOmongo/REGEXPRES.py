import connection_mongo
import config
import json
from pprint import pprint

DB='csvtotab'
'''
with open('regularexp.json', encoding = 'utf8') as json_file:
	REGEXPRES = json.load(json_file)

with open('ddre.json', encoding = 'utf8') as json_file:
	DDRE = json.load(json_file)

with open('ddvs.json', encoding = 'utf8') as json_file:
	DDVS = json.load(json_file)
'''
JSONFILE = [
			['../JSON/regularexp.json','regex'],
			['../JSON/ddre.json','ddre'],
			['../JSON/ddvs.json','ddvs']
			]

def update_regex(dbtype):
	if dbtype == 'mongodb':
		db = connection_mongo.connection_db(host=config.HOST_MONGO, port= config.PORT_MONGO, database = 'csvtotab')
		col_list = db.list_collection_names()

		for file in JSONFILE:

			if file[1] in col_list:
				col = db[file[1]]
				col.drop()


			with open(file[0], encoding = 'utf8') as json_file:
				col = db[file[1]]
				col.insert_many(json.load(json_file))
			
'''
#Here is what the code above does
		if 'ddre' in col_list:
			col = db['ddre']
			col.drop()

		col = db['ddre']
		col.insert_many(DDRE)

		if 'regex' in col_list:
			col = db['regex']
			col.drop()

		col = db['regex']
		col.insert_many(REGEXPRES)

		if 'ddvs' in col_list:
			col = db['ddvs']
			col.drop()

		col = db['ddvs']
		
		col.insert_many(DDVS)

'''




def LD(s, t):
    if s == "":
        return len(t)
    if t == "":
        return len(s)
    if s[-1] == t[-1]:
        cost = 0
    else:
        cost = 1
       
    res = min([LD(s[:-1], t)+1,
               LD(s, t[:-1])+1, 
               LD(s[:-1], t[:-1]) + cost])

    return res

def test():
	#print(LD("Python", "Peithen"))	
	client = connection_mongo.connection_client(host=config.HOST_MONGO, port=config.PORT_MONGO)

	testdb = client['csvtotab']

	print(testdb.list_collection_names())

	update_regex('mongodb')


	print(testdb.list_collection_names())

test()


