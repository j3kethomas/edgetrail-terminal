import mysql.connector

server = mysql.connector.connect(
    host = 'localhost', 
    port = 3306,
    user = 'j3kethomas',
    password = 'skittLES@0511',
    database = 'edgetrail_db'
) 

# Create a cursor to execute SQL
cursor = server.cursor() 

cursor.execute('select * from futures_daily')
result = cursor.fetchall()
print(result) 

cursor.close()
server.close() 