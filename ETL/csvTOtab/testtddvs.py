def villercorri(nb_col):


	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	curs = connection.cursor()

	type_d = get_type_dominant(nb_col)
	if type_d.upper() == 'VARCHAR':

		curs.execute("select OLDVALUES from DR_CSVFILE_COL_{}".format(nb_col))
		t = curs.fetchall()

		curs.execute("select OLDVALUES from DR_CSVFILE_COL_{}".format(nb_col))
		t = curs.fetchall()

		for val in t:


#select OLDVALUES,COUNT(OLDVALUES) FROM DR_CSVFILE_COL_3 WHERE OLDVALUES="PREMIER" GROUP BY OLDVALUES having soundex(OLDVALUES) IN (select soundex(FRENCH) from DDVS)



def check_sementique(tabnum) : 
	connection = connection_mysql.connection_db(host=config.HOST_MYSQL, port= config.PORT, password=config.PASSWD_MYSQL, user=config.USER_MYSQL, db=DB)
	mycursor = connection.cursor()

	mycursor.execute('select OLDVALUES from DR_CSVFILE_COL_{}'.format(tabnum))
	old_lst = mycursor.fetchall()
	mycursor.execute('select * from DDRE')# i DDRE REGULAR by REGULAREXP
	regex_lst = mycursor.fetchall()
	mycursor2 = connection.cursor()
	for data in old_lst :
		if data['OLDVALUES'] != None :
			found=False
			for reg in regex_lst :
				if re.match(reg['regex'],data['OLDVALUES'].upper()):# i changed REGULAR by regex
					#update value
					query= "UPDATE DR_CSVFILE_COL_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES = '{}' ".format(tabnum,reg['cat'],reg['subcat'],data['OLDVALUES'])

					mycursor.execute(query)
					#print(query)
					found= True
					connection.commit()
			if not found :

				mycursor2.execute('select OLDVALUES,COUNT(OLDVALUES) as CB FROM DR_CSVFILE_COL_3 WHERE OLDVALUES="{}" GROUP BY OLDVALUES having soundex(OLDVALUES) IN (select soundex(FRENCH) from DDVS)'.format(data['OLDVALUES']))
					
				if mycursor2.rowcount() > 0 :
					res = mycursor2.fetchone() 
					if res['CB'] > 0:
						mycursor2.execute('select CATEGORY from DDVS where SOUNDEX(FRENCH) = SOUNDEX("{}")'.format(data['OLDVALUES']))
						subtype = mycursor2.fetchone()
						query= "UPDATE DR_CSVFILE_COL_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES = '{}' ".format(tabnum,'VARCHAR',subtype['CATEGORY'],data['OLDVALUES'])

					else: 
						#UNKNOWN
						mycursor.execute("UPDATE DR_CSVFILE_COL_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES = '{}'".format(tabnum,'VARCHAR','UNKNOWN',data['OLDVALUES']))
						connection.commit()
				else:
					mycursor.execute("UPDATE DR_CSVFILE_COL_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES = '{}'".format(tabnum,'UNKNOWN','UNKNOWN',data['OLDVALUES']))
					connection.commit()

		else :
			mycursor.execute("UPDATE DR_CSVFILE_COL_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES is null".format(tabnum,'NULL','NULL'))
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

