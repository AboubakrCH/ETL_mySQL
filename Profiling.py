import mysql.connector
import re
from datetime import date

hname = "localhost"
username = "giitto"
userpassword = "abcd"
dbname = "etl"

def create_m0_table():
	tmpdb = mysql.connector.connect(
 		host="localhost",
	  	user=username,
 		passwd=userpassword,
 		database="etl"
	)
	curs = tmpdb.cursor()

	curs.execute("drop table DR_CSVFILE_TabCol")

	query = "create table DR_CSVFILE_TabCol( REFERENCE TinyText, OLDNAME TinyText, NEWNAME TinyText,"
	query = query + " M00 INT, M100 INT, M101 INT, M102 INT, M103 INT, M104 INT, M105 INT, M106 INT, M107 INT, M108 INT, M109 INT, M110 INT, M111 TinyText,"
	query = query + "M112 INT, M113 INT, M114 TinyText, M115 INT, M116 INT )"
	curs.execute(query)

	tmpdb.close()

def fetch_m(curs,i):
	curs.execute("select count(*) as M000 from DR_CSVFile_Col_{}".format(i))
	m000 = curs.fetchone()

	curs.execute("select count(*) as M100 from DR_CSVFile_Col_{} where OLDVALUES IS NULL".format(i))
	m100 = curs.fetchone()

	curs.execute("select count(*) as M101 from DR_CSVFile_Col_{} where OLDVALUES IS NOT NULL".format(i))
	m101 = curs.fetchone()

	curs.execute("select MIN(COLUMNWIDTH) as M102 from DR_CSVFile_Col_{}".format(i))
	m102 = curs.fetchone()

	curs.execute("select MAX(COLUMNWIDTH) as M103 from DR_CSVFile_Col_{}".format(i))
	m103 = curs.fetchone()

	nb_word = curs.execute("select MAX(NUMBEROFWORDS) as M104 from DR_CSVFile_Col_{}".format(i))
	
	if nb_word !=0:
		m104 = curs.fetchone()
	else:
		m104 = (0,)

	curs.execute("select COUNT(SYNTACTICTYPE) as M105 from DR_CSVFile_Col_{} where UPPER(SYNTACTICTYPE) ='VARCHAR'".format(i))
	m105 = curs.fetchone()

	curs.execute("select COUNT(SYNTACTICTYPE) as M106 from DR_CSVFile_Col_{} where UPPER(SYNTACTICTYPE) ='NUMBER'".format(i))
	m106 = curs.fetchone()

	curs.execute("select COUNT(SYNTACTICTYPE) as M107 from DR_CSVFile_Col_{} where UPPER(SYNTACTICTYPE) ='DATE'".format(i))
	m107 = curs.fetchone()

	curs.execute("select COUNT(SYNTACTICTYPE) as M108 from DR_CSVFile_Col_{} where UPPER(SYNTACTICTYPE) ='BOOLEAN'".format(i))
	m108 = curs.fetchone()

	curs.execute("select COUNT(SYNTACTICTYPE) as M109 from DR_CSVFile_Col_{} where UPPER(SYNTACTICTYPE) ='NULL'".format(i))
	m109 = curs.fetchone()

	curs.execute("select COUNT(DISTINCT OLDVALUES) as M110 from DR_CSVFile_Col_{}".format(i))
	m110 = curs.fetchone()

	curs.execute("SELECT COUNT(DISTINCT OLDVALUES) as M110 from DR_CSVFile_Col_{}".format(i))
	m110 = curs.fetchone()

	curs.execute("SELECT t1.SYNTACTICTYPE FROM (SELECT SYNTACTICTYPE,COUNT(*) as b FROM DR_CSVFile_Col_{} WHERE SYNTACTICTYPE IS NOT NULL GROUP BY SYNTACTICTYPE)t1 where b = (SELECT MAX(a) FROM (SELECT SYNTACTICTYPE,COUNT(*) as a FROM DR_CSVFile_Col_{} WHERE SYNTACTICTYPE IS NOT NULL GROUP BY SYNTACTICTYPE)t2)".format(i,i))
	m111 = curs.fetchone()

	curs.execute("select COUNT(OBSERVATION) as M112 from DR_CSVFILE_COL_{} where OBSERVATION IS NOT NULL".format(i))
	m112 = curs.fetchone()

	m113 = (m000[0] - m112[0],)

	curs.execute("SELECT t1.SEMANTICCATEGORY FROM (SELECT SEMANTICCATEGORY,COUNT(*) as b FROM DR_CSVFile_Col_{} WHERE SEMANTICCATEGORY IS NOT NULL GROUP BY SEMANTICCATEGORY)t1 where b = (SELECT MAX(a) FROM (SELECT SEMANTICCATEGORY,COUNT(*) as a FROM DR_CSVFile_Col_{} WHERE SEMANTICCATEGORY IS NOT NULL GROUP BY SEMANTICCATEGORY)t2)".format(i,i))
	m114 = curs.fetchone()

	curs.execute("select COUNT(SEMANTICCATEGORY) from DR_CSVFILE_COL_{} where SEMANTICCATEGORY != '{}'".format(i,m114[0]))
	m115 = curs.fetchone()

	curs.execute("select COUNT(SEMANTICCATEGORY) from DR_CSVFILE_COL_{} where SEMANTICCATEGORY = '{}'".format(i,m114[0]))
	m116 = curs.fetchone()

	curs.execute("")

	query = "INSERT INTO DR_CSVFile_TabCol VALUES('CSV_File_{}','Col{}' ,'Col{}_{}',".format(date.today(),i,i,i,m114[0])
	print("1")
	query = query + " {}, {}, {}, {}, {},".format(m000[0],m100[0],m101[0],m102[0],m103[0])
	print("2")
	query = query + " {}, {}, {}, {}, {},".format(m104[0],m105[0],m106[0],m107[0],m108[0])
	print("3")
	query = query + " {}, {}, '{}', {}, {},".format(m109[0],m110[0],m111[0],m112[0],m113[0])
	print("4")
	query = query + " '{}', {}, {})".format(m114[0],m115[0],m116[0])
	#print(query)

	return query

def create_m0(nb_col):

	tmpdb = mysql.connector.connect(
 		host=hname,
	  	user=username,
 		passwd=userpassword,
 		database=dbname
	) #Create the connector
	curs = tmpdb.cursor() #create the cursor

	create_m0_table() #Initialize the DR_CSVFileTabCol table

	for i in range(1,nb_col+1): #For each column of the table we do the recap of the data
		#print(i)  #Print the step of the process
		query = fetch_m(curs,i) #Create the queru for the insert
		curs.execute(query) #Insert in the DR_CSVFileTabCol

	tmpdb.commit() #Commit the change if we don't commit the table will not be updated
	tmpdb.close() #Close the connector






mydb = mysql.connector.connect(
  host=hname,
  user=username,
  passwd=userpassword,
  database=dbname
)

cursor = mydb.cursor()

create_m0(1)

cursor.execute("select * from DR_CSVFile_TabCol")

results = cursor.fetchall()

for result in results:
	print(result)

mydb.close()