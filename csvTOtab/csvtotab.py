#!/usr/bin/env python
import csv
import time 
import connection_mysql
import config
from datetime import datetime
import re
from datetime import date

DB = 'csvtotab'
table_name = 'CSV2TABCOLUMNS'
def open_csvfile(path_file, delimiter):
	data = []
	max_column=0
	with open(path_file) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=delimiter)
		for row in csv_reader:
			line = {}
			line = {'REFERENCE':'CSVFILE_{}'.format("__".join(row))}
			#line = {'REFERENCE':'CSVFILE_'}
			i = 1
			for v in row : 
				line['COL{}'.format(i)]= v
				i+=1
			data.append(line)
			if len(row) > max_column : 
				max_column = len(row)
	return max_column, data

def tab_struct(nbr_column):
	
	struct = {'REFERENCE':''}
	cpt=1
	for i in range(nbr_column):
		struct['COL{}'.format(i+1)]=''
	return struct

def _generate_query(structure, table_name):
   """
   Generate the request according to the structure
   """
   names = list(structure)
   cols = ', '.join(map(_escape_name, names))  # assumes the keys are valid column names.
   placeholders = ', '.join(['%({})s'.format(name) for name in names])
   query = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name, cols, placeholders)
   return query

def _escape_name(s):
   """Escape name to avoid SQL injection and keyword clashes.
   Doubles embedded backticks, surrounds the whole in backticks.
   Note: not security hardened, caveat emptor.
   """
   return '`{}`'.format(s.replace('`', '``'))

#------------------------------------------------------------
#----------------------- W O R K ----------------------------
#------------------------------------------------------------

def clean_database():
	#connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	query = 'select table_name from meta_csvtotab'
	with connection.cursor() as cursor:
		a = cursor.execute(query)
		myresult = cursor.fetchall()
		for x in myresult:
			table_name = x['table_name']
			query = 'DROP TABLE {}'.format(table_name)
			b = cursor.execute(query)
		query = 'DELETE FROM meta_csvtotab'
		b = cursor.execute(query)
	connection.commit()

def create_table(nbr_column, table_name): 
	sql_create = 'CREATE TABLE {} ('.format(table_name)
	sql_insert_in_meta = 'INSERT INTO meta_csvtotab values("{}")'.format(table_name)
	sql_create += 'REFERENCE tinytext, '
	for i in range(nbr_column-1):
		sql_create+='COL{} tinytext,'.format(i+1)
	sql_create+='COL{} tinytext)'.format(nbr_column)
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	with connection.cursor() as cursor:
		cursor.execute('SET SESSION wait_timeout=8000;')
		l = cursor.execute(sql_create)
		l = cursor.execute(sql_insert_in_meta)
	connection.commit()

def insert_db(nbr_column, data):
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	sql = _generate_query(tab_struct(nbr_column), table_name)
	with connection.cursor() as cursor:
			cursor.execute('SET SESSION wait_timeout=8000;')
			l = cursor.executemany(sql,data)
	connection.commit()
	connection.close()

def update_data(nbr_column, data):
	all_data = []
	for element in data:
		struct = tab_struct(nbr_column)
		for key in struct :
			struct[key]= element[key] if key in element else ''
		all_data.append(struct)
	return all_data

def create_DR_CSVFILE_COL(nbr_column):
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	for i in range(nbr_column):
		sql_create = 'CREATE TABLE DR_CSVFILE_COL_{} (REFERENCE tinytext,	OLDVALUES tinytext,	SYNTACTICTYPE tinytext, SUBSYNTACTICTYPE tinytext, COLUMNWIDTH tinytext, NUMBEROFWORDS tinytext, OBSERVATION 	tinytext   , NEWVALUES	tinytext    ,SEMANTICCATEGORY	tinytext,   SEMANTICSUBCATEGORY tinytext)'.format(i+1)
		sql_insert_in_meta = 'INSERT INTO meta_csvtotab values("DR_CSVFILE_COL_{}")'.format(i+1)
		#print(sql_create)
		#print(sql_insert_in_meta)
		with connection.cursor() as cursor:
			cursor.execute('SET SESSION wait_timeout=8000;')
			b = cursor.execute(sql_create)
			c = cursor.execute(sql_insert_in_meta)
			d = cursor.execute('select COL{} from {}'.format(i+1, table_name))
			myresult = cursor.fetchall()
			for x in myresult:
				OLDVALUES = x['COL{}'.format(i+1)]
				type_ = get_type(OLDVALUES) if len(OLDVALUES) else ('UNKOWN','UNKOWN')
				SYNTACTICTYPE ,SUBSYNTACTICTYPE = type_[0], type_[1]
				#print(x['COL{}'.format(i+1)] ,' ---> ', SYNTACTICTYPE)
				query_insert = "INSERT INTO DR_CSVFILE_COL_{} (REFERENCE, OLDVALUES, SYNTACTICTYPE,SUBSYNTACTICTYPE, COLUMNWIDTH, NUMBEROFWORDS) VALUES ( 'CSVfile_{}_Col{}', '{}', '{}', '{}' , '{}', '{}' )".format(i+1,str(datetime.today().date()),i+1, OLDVALUES, SYNTACTICTYPE, SUBSYNTACTICTYPE, str(len(OLDVALUES)), str(len(str(OLDVALUES).split(' '))))
				#print(query_insert)
				cursor.execute(query_insert)
		 #time.sleep(5)
		connection.commit()

def get_type(txt):
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	with connection.cursor() as cursor:
		cursor.execute('select type, subtype, regex from REGULAREXP')
		myREGEX = cursor.fetchall()
		for tuple_ in myREGEX:
			type_ = tuple_['type']
			subtype = tuple_['subtype']
			regex = tuple_['regex']
			x = re.match(regex, txt)
			if x : 
				return type_,subtype
	return 'UNKOWN','UNKOWN'

#-------------------------------------------BOOBA ---------------------------

def get_dominant_any(tabname,col_name):
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	mycursor = connection.cursor()
	_query = 'SELECT  {},COUNT({}) AS value_occur FROM  {} GROUP BY {} ORDER BY value_occur DESC'.format(col_name,col_name,tabname,col_name)
	mycursor.execute(_query)
	tables = mycursor.fetchone()
	connection.close()
	return(tables[0])


def get_type_dominant(tabnum):
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	mycursor = connection.cursor()
	_query = 'SELECT  SUBSYNTACTICTYPE,COUNT(SUBSYNTACTICTYPE) AS value_occur FROM  DR_CSVFILE_COL_{} GROUP BY SUBSYNTACTICTYPE ORDER BY value_occur DESC'.format(tabnum)
	mycursor.execute(_query)
	tables = mycursor.fetchone()
	connection.close()
	return(tables['SUBSYNTACTICTYPE'])


def detect_anomaly(num):
	#connection to database
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)

	mycursor = connection.cursor()

	dominant = get_type_dominant(num)#get doinant type in DR_CSVFILE_COL_num
	
	"""
	in folowing lines we select data from the current DR_CSVFILE_COL_ table
	then compare each SUBSYNTACTICTYPE whith the dominant type to detect anomaly
	and also update new values 
	"""
	_query_ = "UPDATE DR_CSVFILE_COL_{} SET OBSERVATION =CONCAT(OLDVALUES,'<?>Anomaly') where SUBSYNTACTICTYPE != '{}'".format(num,dominant)
	#print(_query_)
	mycursor.execute(_query_)

	_query_null = "UPDATE DR_CSVFILE_COL_{} SET OBSERVATION ='NULL<?>Anomaly' where OLDVALUES IS NULL ".format(num) 
	#print(_query_null)
	mycursor.execute(_query_null)
	connection.commit()
	connection.close()


def new_val(tabnum):
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)

	mycursor = connection.cursor() 
	query_new = "UPDATE DR_CSVFILE_COL_{} SET NEWVALUES = OLDVALUES where OBSERVATION IS  NULL ".format(tabnum)
	mycursor.execute(query_new)

	query_anomaly = "UPDATE DR_CSVFILE_COL_{} SET NEWVALUES =OBSERVATION where OBSERVATION IS NOT NULL ".format(tabnum)
	mycursor.execute(query_anomaly)
	connection.commit()
	connection.close()


def check_sementique(tabnum) : 
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	mycursor = connection.cursor()

	mycursor.execute('select OLDVALUES from DR_CSVFILE_COL_{}'.format(tabnum))
	old_lst = mycursor.fetchall()
	mycursor.execute('select * from REGULAREXP')# i DDRE REGULAR by REGULAREXP
	regex_lst = mycursor.fetchall()
	mycursor2 = connection.cursor()
	for data in old_lst :
		if data['OLDVALUES'] != None :
			found=False
			for reg in regex_lst :
				if re.match(reg['regex'],data['OLDVALUES']):# i changed REGULAR by regex
					#update value
					query= "UPDATE DR_CSVFILE_COL_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES = '{}' ".format(tabnum,reg['type'],reg['subtype'],data['OLDVALUES'])

					mycursor.execute(query)
					#print(query)
					found= True
					connection.commit()
			if not found : 
				#Unknown
				mycursor.execute("UPDATE DR_CSVFILE_COL_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES = '{}'".format(tabnum,'Unknown','Unknown',data['OLDVALUES']))
				connection.commit()
		else :
			mycursor.execute("UPDATE DR_CSVFILE_COL_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES is null".format(tabnum,'Unknown','Unknown'))
			connection.commit()
	connection.close()

def update_DR_CSVFILE_COL(nbr_column):
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)

	mycursor = connection.cursor()
	for i in range(nbr_column):
		detect_anomaly(i+1)
		new_val(i+1)
		check_sementique(i+1)

	connection.close()


#------------------ KHALID -------------------------------

def create_m0_table():
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	curs = connection.cursor()

	#curs.execute("drop table DR_CSVFILE_TabCol")
	query = "create table DR_CSVFILE_TabCol( REFERENCE TinyText, OLDNAME TinyText, NEWNAME TinyText,"
	query = query + " M00 INT, M100 INT, M101 INT, M102 INT, M103 INT, M104 INT, M105 INT, M106 INT, M107 INT, M108 INT, M109 INT, M110 INT, M111 TinyText,"
	query = query + "M112 INT, M113 INT, M114 TinyText, M115 INT, M116 INT )"
	curs.execute(query)
	sql_insert_in_meta = 'INSERT INTO meta_csvtotab values("DR_CSVFILE_TabCol")'
	curs.execute(sql_insert_in_meta)
	connection.commit()
	connection.close()


def fetch_m(i):
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	curs = connection.cursor()
	
	curs.execute("select count(*) as M000 from DR_CSVFILE_COL_{}".format(i))
	m000 = curs.fetchone()
	m000 = m000['M000']

	curs.execute("select count(*) as M100 from DR_CSVFILE_COL_{} where OLDVALUES IS NULL".format(i))
	m100 = curs.fetchone()
	m100=m100['M100']
	

	curs.execute("select count(*) as M101 from DR_CSVFILE_COL_{} where OLDVALUES IS NOT NULL".format(i))
	m101 = curs.fetchone()
	m101=m101['M101']
	
	curs.execute("select MIN(COLUMNWIDTH) as M102 from DR_CSVFILE_COL_{}".format(i))
	m102 = curs.fetchone()
	m102=m102['M102']

	curs.execute("select MAX(COLUMNWIDTH) as M103 from DR_CSVFILE_COL_{}".format(i))
	m103 = curs.fetchone()
	m103=m103['M103']
	

	nb_word = curs.execute("select MAX(NUMBEROFWORDS) as M104 from DR_CSVFILE_COL_{}".format(i))
	
	if nb_word !=0:
		m104 = curs.fetchone()
		m104 = m104['M104']
	else:
		m104 = 0

	
	curs.execute("select COUNT(SYNTACTICTYPE) as M105 from DR_CSVFILE_COL_{} where UPPER(SYNTACTICTYPE) ='VARCHAR'".format(i))
	m105 = curs.fetchone()
	m105 = m105['M105']

	curs.execute("select COUNT(SYNTACTICTYPE) as M106 from DR_CSVFILE_COL_{} where UPPER(SYNTACTICTYPE) ='NUMBER'".format(i))
	m106 = curs.fetchone()
	m106 = m106['M106']

	curs.execute("select COUNT(SYNTACTICTYPE) as M107 from DR_CSVFILE_COL_{} where UPPER(SYNTACTICTYPE) ='DATE'".format(i))
	m107 = curs.fetchone()
	m107 = m107['M107']

	curs.execute("select COUNT(SYNTACTICTYPE) as M108 from DR_CSVFILE_COL_{} where UPPER(SYNTACTICTYPE) ='BOOLEAN'".format(i))
	m108 = curs.fetchone()
	m108 = m108['M108']

	curs.execute("select COUNT(SYNTACTICTYPE) as M109 from DR_CSVFILE_COL_{} where UPPER(SYNTACTICTYPE) ='NULL'".format(i))
	m109 = curs.fetchone()
	m109 = m109['M109']

	curs.execute("select COUNT(DISTINCT OLDVALUES) as M110 from DR_CSVFILE_COL_{}".format(i))
	m110 = curs.fetchone()
	m110 = m110['M110']


	curs.execute("SELECT t1.SYNTACTICTYPE FROM (SELECT SYNTACTICTYPE,COUNT(*) as b FROM DR_CSVFILE_COL_{} WHERE SYNTACTICTYPE IS NOT NULL GROUP BY SYNTACTICTYPE)t1 where b = (SELECT MAX(a) FROM (SELECT SYNTACTICTYPE,COUNT(*) as a FROM DR_CSVFILE_COL_{} WHERE SYNTACTICTYPE IS NOT NULL GROUP BY SYNTACTICTYPE)t2)".format(i,i))
	m111 = curs.fetchone()
	m111 = m111['SYNTACTICTYPE']
	

	curs.execute("select COUNT(OBSERVATION) as M112 from DR_CSVFILE_COL_{} where OBSERVATION IS NOT NULL".format(i))
	m112 = curs.fetchone()
	m112 = m112['M112']
	m113 = m000 - m112
	

	curs.execute("SELECT t1.SEMANTICCATEGORY FROM (SELECT SEMANTICCATEGORY,COUNT(*) as b FROM DR_CSVFILE_COL_{} WHERE SEMANTICCATEGORY IS NOT NULL GROUP BY SEMANTICCATEGORY)t1 where b = (SELECT MAX(a) FROM (SELECT SEMANTICCATEGORY,COUNT(*) as a FROM DR_CSVFILE_COL_{} WHERE SEMANTICCATEGORY IS NOT NULL GROUP BY SEMANTICCATEGORY)t2)".format(i,i))
	m114 = curs.fetchone()
	m114 = m114['SEMANTICCATEGORY']

	curs.execute("select COUNT(SEMANTICCATEGORY) as cpt from DR_CSVFILE_COL_{} where SEMANTICCATEGORY != '{}'".format(i,m114))
	m115 = curs.fetchone()
	m115 = m115['cpt']

	curs.execute("select COUNT(SEMANTICCATEGORY) as cpt from DR_CSVFILE_COL_{} where SEMANTICCATEGORY = '{}'".format(i,m114))
	m116 = curs.fetchone()
	m116 = m116['cpt']

	curs.execute("select SEMANTICSUBCATEGORY from DR_CSVFILE_COL_{} where OBSERVATION is NULL".format(i))
	SEMANTICSUBCATEGORY = curs.fetchone()
	
	SEMANTICSUBCATEGORY = SEMANTICSUBCATEGORY['SEMANTICSUBCATEGORY']

	query = "INSERT INTO DR_CSVFILE_TabCol VALUES('CSV_File_{}','Col{}' ,'Col_{}',".format(date.today(),i,SEMANTICSUBCATEGORY)
	query = query + " {}, {}, {}, {}, {},".format(m000,m100,m101,m102,m103)
	query = query + " {}, {}, {}, {}, {},".format(m104,m105,m106,m107,m108)
	query = query + " {}, {}, '{}', {}, {},".format(m109,m110,m111,m112,m113)
	query = query + " '{}', {}, {})".format(m114,m115,m116)
	curs.close()
	return query


def create_m0(nb_col):

	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	curs = connection.cursor() #create the cursor
	create_m0_table() #Initialize the DR_CSVFileTabCol table

	for i in range(nb_col): #For each column of the table we do the recap of the data
		query = fetch_m(i+1) #Create the queru for the insert
		curs.execute(query) #Insert in the DR_CSVFileTabCol

	connection.commit() #Commit the change if we don't commit the table will not be updated
	connection.close() #Close the connector

def clean_pure_double(data):

	noneDupData = []
	noneDupData.append(data[0]) #We  pute the first element of the data in the list

	#For each element in the data we check if the element is already in the list we don't re-add it, 
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
			#print(x[key].replace("''"," "))
			#print(x[key].replace("'"," "))
			x[key] = x[key].replace("''"," ")
			x[key] = x[key].replace("'"," ")
			x[key] = x[key].replace("\""," ")
		
		corrected_data.append(x)

	return corrected_data



# ----------------------------- MAIN ------------------------
def main():
	started_time = time.time() 
	print("\n\n######  STARTED  ######")

	print("etape 1: préparation des données:")
	nbr_column, data = open_csvfile(path_file='C:/wamp64/www/APP/file_uploaded/csvfile.csv',delimiter=';')
	print("Recherche de double pure:")
	data = clean_pure_double(data)
	print("Recherche de guillemet")
	#data = clean_quote(data)
	#print(data,'\n')


	
	print("\n\nBD: Vidage de la BD précedente")
	clean_database()

	print("BD: Creation des tables de la nouvelle BD")
	create_table(nbr_column, table_name)

	print("BD: insertion des donnée")
	insert_db(nbr_column, update_data(nbr_column, data))


	
	print("\n\nprofiling: création des tables par colonne")
	create_DR_CSVFILE_COL(nbr_column)

	print("profiling: analyse des colonnes")
	update_DR_CSVFILE_COL(nbr_column)

	print("profiling: profiling des données")
	create_m0(nbr_column)


	print("\nFINISHED IN {} SECONDS\n".format(round(time.time()-started_time,2)))
	
main()

