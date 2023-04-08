import mysql.connector

cnx = mysql.connector.connect(
    user='root',
    password='N01D_2',
    host='127.0.0.1',
    database='bad_sakila',
    auth_plugin='mysql_native_password')

cursor = cnx.cursor()
query = ("SELECT title, rating, original_language_id, COALESCE(original_language_id,'unknown') AS OriginalLanguage FROM film;")
cursor.execute(query)

# print all the rows
for row in cursor.fetchall():
    print(row)

cursor.close()
cnx.close()

