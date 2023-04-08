import mysql.connector

cnx = mysql.connector.connect(user='root',
    password='',
    host='127.0.0.1',
    database='education',
    auth_plugin='mysql_native_password')

cursor = cnx.cursor()
query = ("SELECT * FROM colleges")
cursor.execute(query)

# print all the rows
for row in cursor.fetchall():
    print(row)

cursor.close()
cnx.close()