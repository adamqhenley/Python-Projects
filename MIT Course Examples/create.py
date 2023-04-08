import mysql.connector

cnx = mysql.connector.connect(user='root',
    password='',
    host='127.0.0.1',
    database='education',
    auth_plugin='mysql_native_password')

cursor = cnx.cursor()

# get user input from terminal:
college = input('Enter college name: ')
students = input('Enter student population: ')

# create new row
query = (f'INSERT INTO `Colleges` VALUES(NULL,"{college}",{students},NULL,NULL,NULL)')
cursor.execute(query)


# read all entries
query = ("SELECT * FROM colleges")
cursor.execute(query)

# print all the rows
for row in cursor.fetchall():
    print(row)

cursor.close()
cnx.close()

