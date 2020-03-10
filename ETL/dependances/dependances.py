import pymysql.cursors  

def getTable(connection, table_name):
    sql = 'SELECT  * FROM ' + table_name
    
    cursor = connection.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()   
    
    return (records)

  
def generateDependencies(connection,table_name):
    # SQL 
    selected_values = ' COLUMN_NAME '
    where_conditions = " where TABLE_NAME like  '%"+table_name+"%' "

    sql = 'SELECT ' + selected_values + ' FROM information_schema.columns ' + where_conditions 
     
    # Execute query.
    cursor = connection.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()    
 
 
    for row_left in records:
        col_left = row_left['COLUMN_NAME']
        for row_right in records:
            col_right = row_right['COLUMN_NAME']
            if(col_left.upper() != col_right.upper() ):
                print(col_left + ' -- '+ col_right)
                args = (table_name,'DR_SemanticDependencies',col_left,col_right,0);
                cursor_2 = connection.cursor()
                cursor_2.callproc('VERIFDF', args)
        print('--------------')
    args = (table_name,'DR_SemanticDependencies');
    cursor_3 = connection.cursor()
    cursor_3.callproc('COMBINING_LEFTCOL', args)
    
    connection.commit()

        
       
# Connect to the database.
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='allahuwahid1',                             
                             db='etl',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
 
print ("connect successful!!")
try:
  # ajouter un delete de la table DR
  generateDependencies(connection,'TABLACONTROLER')   
  #generateDependencies(connection,'DICOMOIS')
  data = getTable(connection, 'DR_SemanticDependencies')
  print(data)   
  print(len(data)) 
finally:
    # Close connection.
    connection.close()
    