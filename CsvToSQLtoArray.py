import mysql.connector
import re

username="giitto"
userpassword="abcd"

mydb = mysql.connector.connect(
  host="localhost",
  user=username,
  passwd=userpassword,
  database="etl"
)

def getSemanticType(string):
	tmpdb = mysql.connector.connect(
 		host="localhost",
	  	user=username,
 		passwd=userpassword,
 		database="etl"
	)
	curs = tmpdb.cursor()

	curs.execute("select SUBTYPE, TYPE, CATEGORY, SUBCATEGORY, REGULAREXPRESSION from DDRE WHERE REGULAREXPRESSION = \'([0-2][0-9]|3[0-1])(-|/)(0[0-9]|1[0-2])(-|/)[0-9]{4}\'")

	res = curs.fetchall()

	cat = "INCONNU"
	subcat = "INCONNU"
	typ = "INCONNU"
	subtyp = "INCONNU"

	for SUBTYPE, TYPE, CATEGORY , SUBCATEGORY, REGULAREXPRESSION in res:
		matchreg = re.match(REGULAREXPRESSION,string)
		print(REGULAREXPRESSION)
		if matchreg:
			cat = CATEGORY
			subcat = SUBCATEGORY
			typ = TYPE
			subtyp = SUBTYPE


			tmpdb.close()
			return subtyp, typ, cat , subcat

	tmpdb.close()
	return subtyp, typ, cat , subcat




cursor = mydb.cursor()

cursor.execute("select REFERENCE, OLDVALUES, SYNTACTICTYPE, SUBSYNTACTITYPE, COLUMNWIDTH, NUMBEROFWORDS, OBSERVATION, NEWVALUES, SEMANTICCATEGORY, SEMANTICSUBCATEGORY from DR_CSVFile_Col_1")

result = cursor.fetchall()

cursor.execute("drop table DR_CSVFILE_TabCol")
cursor.execute("create table DR_CSVFILE_TabCol( REFERENCE VARCHAR(100), OLDNAME VARCHAR(100), NEWNAME VARCHAR(100), M00 INT, M100 INT, M101 INT, M102 INT, M103 INT, M104 INT, M105 INT, M106 INT, M107 INT, M108 INT, M109 INT, M110 INT, M111 VARCHAR(100), M112 INT, M113 INT, M114 VARCHAR(100), M115 INT, M116 INT )")




for REFERENCE, OLDVALUES, SYNTACTICTYPE, SUBSYNTACTITYPE, COLUMNWIDTH, NUMBEROFWORDS, OBSERVATION, NEWVALUES, SEMANTICCATEGORY, SEMANTICSUBCATEGORY in result:
	#print("from {} we had {} and we replace with {}".format(REFERENCE,OLDVALUES,NEWVALUES))
	print(getSemanticType(NEWVALUES))


mydb.close()