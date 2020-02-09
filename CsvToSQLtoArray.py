import mysql.connector

username="giitto"
userpassword="abcd"

mydb = mysql.connector.connect(
  host="localhost",
  user=username,
  passwd=userpassword,
  database="etl"
)

cursor = mydb.cursor()

cursor.execute("show tables")

for x in cursor:
	print(x)
