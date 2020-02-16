import mysql.connector 
import re

# Connect to the database.

#mycursor.execute('select * from DR_CSVFile_Col_1')

#tables = mycursor.fetchall()

#for res in tables :
#    print(res)


def get_dominant_any(tabname,col_name):
    db = mysql.connector.connect(host='localhost',
                        user='root',
                        password='allahuwahid1',                             
                        db='etl')

    mycursor = db.cursor()
    _query = 'SELECT  {},COUNT({}) AS value_occur FROM  {} GROUP BY {} ORDER BY value_occur DESC'.format(col_name,col_name,tabname,col_name)
    mycursor.execute(_query)
    tables = mycursor.fetchone()
    db.close()
    return(tables[0])

def get_type_dominant(tabnum):
    db = mysql.connector.connect(host='localhost',
                        user='root',
                        password='allahuwahid1',                             
                        db='etl')

    mycursor = db.cursor()
    _query = 'SELECT  SUBSYNTACTICTYPE,COUNT(SUBSYNTACTICTYPE) AS value_occur FROM  DR_CSVFile_Col_{} GROUP BY SUBSYNTACTICTYPE ORDER BY value_occur DESC'.format(tabnum)
    mycursor.execute(_query)
    tables = mycursor.fetchone()
    db.close()
    return(tables[0])

def detect_anomaly(num):
    #connection to database
    db = mysql.connector.connect(host='localhost',
                        user='root',
                        password='allahuwahid1',                             
                        db='etl')

    mycursor = db.cursor()

    dominant = get_type_dominant(num)#get doinant type in DR_CSVFile_Col_num
    
    """
    in folowing lines we select data from the current DR_CSVFile_Col_ table
    then compare each SUBSYNTACTICTYPE whith the dominant type to detect anomaly
    and also update new values 
    
    get_data='select OLDVALUES,SUBSYNTACTICTYPE from DR_CSVFile_Col_{}'.format(num)

    mycursor.execute(get_data)
    tables = mycursor.fetchall()
    for res in tables :
        print(res)
    """
    _query_ = "UPDATE DR_CSVFile_Col_{} SET OBSERVATION =CONCAT(OLDVALUES,'<?>Anomaly') where SUBSYNTACTICTYPE != '{}'".format(num,dominant)
    #print(_query_)
    mycursor.execute(_query_)

    _query_null = "UPDATE DR_CSVFile_Col_{} SET OBSERVATION ='NULL<?>Anomaly' where OLDVALUES IS NULL ".format(num) 
    #print(_query_null)
    mycursor.execute(_query_null)
    db.commit()
    db.close()

def new_val(tabnum):
   #connection to database
    db = mysql.connector.connect(host='localhost',
                        user='root',
                        password='allahuwahid1',                             
                        db='etl')

    mycursor = db.cursor() 
    query_new = "UPDATE DR_CSVFile_Col_{} SET NEWVALUES = OLDVALUES where OBSERVATION IS  NULL ".format(tabnum)
    #print(_query_)
    mycursor.execute(query_new)

    query_anomaly = "UPDATE DR_CSVFile_Col_1{} SET NEWVALUES =OBSERVATION where OBSERVATION IS NOT NULL ".format(tabnum)
    #print(_query_null)
    mycursor.execute(query_anomaly)
    db.commit()
    db.close()

def check_sementique(tabnum) : 
    db = mysql.connector.connect(host='localhost',
                        user='root',
                        password='allahuwahid1',                             
                        db='etl')

    mycursor = db.cursor()

    mycursor.execute('select OLDVALUES from DR_CSVFile_Col_{}'.format(tabnum))
    old_lst = mycursor.fetchall()
    mycursor.execute('select * from DDRE')
    regex_lst = mycursor.fetchall()
    mycursor2 = db.cursor()
    for data in old_lst :
        if data[0] != None :
            print (data[0])
            found=False
            for reg in regex_lst :
                if re.match(reg[2],data[0]):
                    #update value
                    query= "UPDATE DR_CSVFile_Col_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES = '{}' ".format(tabnum,reg[0],reg[1],data[0])

                    mycursor.execute(query)
                    print(query)
                    found= True
                    db.commit()
            if not found : 
                #Unknown
                mycursor.execute("UPDATE DR_CSVFile_Col_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES = '{}'".format(tabnum,'Unknown','Unknown',data[0]))
                db.commit()
        else :
            mycursor.execute("UPDATE DR_CSVFile_Col_{} SET SEMANTICCATEGORY= '{}', SEMANTICSUBCATEGORY = '{}' where OLDVALUES is null".format(tabnum,'Unknown','Unknown'))
            db.commit()
    db.close()




def main():
    db = mysql.connector.connect(host='localhost',
                        user='root',
                        password='allahuwahid1',                             
                        db='etl')

    mycursor = db.cursor()
    #get_type_dominant(1)
    #get_dominant_any( 'DR_CSVFile_Col_1','SUBSYNTACTICTYPE')
    #detect_anomaly(1)
    #new_val(1)
    check_sementique(1)
    mycursor.execute('select * from DR_CSVFile_Col_1')
    tables = mycursor.fetchall()
    for res in tables :
        print(res)


    db.close()

main()









