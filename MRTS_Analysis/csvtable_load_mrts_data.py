import mysql.connector
import pandas as pd
import yaml


###################################################################
# setup database connection
###################################################################

dbinfo = yaml.safe_load(open('db.yaml'))
dbconfig = {
    'user':             dbinfo['user'],
    'password':         dbinfo['pwrd'],
    'host':             dbinfo['host'],
    'database':         dbinfo['db'],
    'auth_plugin':      'mysql_native_password'
}

db_connection_string = mysql.connector.connect(**dbconfig)
cursor = db_connection_string.cursor()



###################################################################
# import csv into pandas df
###################################################################

df_testdata = pd.read_csv('data/testdata.csv')
df_testdata.head()



#dfcsv = pd.read_csv('data/testdata.csv')


#print(dfcsv)

#testobj = {'CsvID','Col2','Col3','Col4','Col5','Col6'}
arrObjs_testdata = []
for i in range(0,len(df_testdata)):
    newrowObj = {}
    for c in df_testdata.columns:
        newrowObj[c] = df_testdata[c].loc[i]
        #print(dfcsv[c].loc[i])
    arrObjs_testdata.append(newrowObj)

print(arrObjs_testdata)


dbkeys = list(arrObjs_testdata[0].keys())
#for k in dbkeys:
    #print(k)

lastcol = dbkeys[-1]
#print(lastcol)


###################################################################
# setup queries
###################################################################

queries = []

q1 = "DROP DATABASE IF EXISTS `csvtestdb`;"
q2 = "CREATE DATABASE IF NOT EXISTS `csvtestdb`;"
q3 = "USE `csvtestdb`;"

queries.append(q1)
queries.append(q2)
queries.append(q3)

#query for dropping csvtable (if it exists)
queryString = "DROP TABLE IF EXISTS `csvtable`;"
queries.append(queryString)

#query for creating table csvtable
queryString = "CREATE TABLE csvtable ("
queryString = queryString + "ID int NOT NULL,"
queryString = queryString + "Col2 varchar (20) NULL, "
queryString = queryString + "Col3 varchar (20) NULL, "
queryString = queryString + "Col4 varchar (20) NULL, "
queryString = queryString + "Col5 varchar (20) NULL, "
queryString = queryString + "Col6 varchar (20) NULL "
queryString = queryString + ") ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=utf8mb4_0900_ai_ci;"
queries.append(queryString)
#print(queryString)

# query for entering data into table
for i in range(0,len(arrObjs_testdata)):
    rec = arrObjs_testdata[i]
    queryString = 'INSERT INTO `csvtable` VALUES('
    for key in dbkeys:
        queryString += str(rec[key])
        if(key != lastcol): # hard-coded last column... TODO: consider a way to generalize the last key
            queryString += ','
    queryString += (')')
    queries.append(queryString)
#print('\n\n')
#print(queries)
#print('\n\n')




###################################################################
# execute queries
###################################################################

for i in range(0,len(queries)):
    print(f'\n\nRun #: {i}:\n')
    print(f'{queries[i]}\n\n')
    cursor.execute(queries[i])







###################################################################
# commit changes
# ref: https://stackoverflow.com/questions/69078992/not-saving-data-mysql-connector-python
###################################################################

db_connection_string.commit()






###################################################################
# verify query worked
###################################################################

query = ("SELECT * FROM csvtable")
#cursor.execute(query)

df = pd.read_sql(query, con= db_connection_string)

print(df)





###################################################################
# close the cursor and db connection
###################################################################

cursor.close()
db_connection_string.close()