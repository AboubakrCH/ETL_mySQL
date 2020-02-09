import csv
import time 
import connection_mysql
import config


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

def create_table(nbr_column, table_name): 
	sql_delete = 'DROP table {}'.format(table_name)
	sql_create = 'CREATE TABLE {} ('.format(table_name)
	sql_create += 'REFERENCE tinytext, '
	for i in range(nbr_column-1):
		sql_create+='COL{} tinytext,'.format(i+1)
	sql_create+='COL{} tinytext)'.format(nbr_column)
	print(sql_create)
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	with connection.cursor() as cursor:
		cursor.execute('SET SESSION wait_timeout=8000;')
		l = cursor.execute(sql_delete)
		l = cursor.execute(sql_create)

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

def insert_db(nbr_column, data):
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	print(data)
	sql = _generate_query(tab_struct(nbr_column), table_name)
	print(sql)
	with connection.cursor() as cursor:
			cursor.execute('SET SESSION wait_timeout=8000;')
			l = cursor.executemany(sql,(data))
	connection.commit()
	connection.close()

def main():
	nbr_column, data = open_csvfile(path_file='csvfile.csv',delimiter=';')
	create_table(nbr_column, table_name)
	insert_db(nbr_column, data)


main()



