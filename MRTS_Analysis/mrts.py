import mysql.connector
import pandas as pd
import yaml


###################################################################
# setup database connection
###################################################################

dbinfo = yaml.safe_load(open('MRTS_Analysis/db.yaml'))
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

df_mrts = pd.read_csv('MRTS_Analysis/data/MRTS_all.csv')
#print(df_mrts.head())
print(df_mrts.head())
print(df_mrts.shape)

#dfcsv = pd.read_csv('data/testdata.csv')


#print(dfcsv)

#testobj = {'CsvID','Col2','Col3','Col4','Col5','Col6'}
arrObjs = []
for i in range(0,len(df_mrts)):
    newrowObj = {}
    for c in df_mrts.columns:
        if(c == 'Description'):
            newrowObj[c] = f'"{df_mrts[c].loc[i]}"'
        else:
            newrowObj[c] = df_mrts[c].loc[i]
        #print(dfcsv[c].loc[i])
    arrObjs.append(newrowObj)

#print(arrObjs)
print(arrObjs[0])

dbkeys = list(arrObjs[0].keys())
lastcol = dbkeys[-1]


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

#query for dropping mrts (if it exists)
queryString = "DROP TABLE IF EXISTS `mrts`;"
queries.append(queryString)

#query for creating table mrts
queryString = "CREATE TABLE mrts ("
queryString = queryString + "ID int NOT NULL, "
queryString = queryString + "NAICS_Code_1 int NULL, "
queryString = queryString + "NAICS_Code_2 int NULL, "
queryString = queryString + "NAICS_Code_3 int NULL, "
queryString = queryString + "Description varchar (200) NULL, "
queryString = queryString + "Adjusted int NULL, "
queryString = queryString + "Year int NULL, "
queryString = queryString + "January DECIMAL NULL, "
queryString = queryString + "February DECIMAL NULL, "
queryString = queryString + "March DECIMAL NULL, "
queryString = queryString + "April DECIMAL NULL, "
queryString = queryString + "May DECIMAL NULL, "
queryString = queryString + "June DECIMAL NULL, "
queryString = queryString + "July DECIMAL NULL, "
queryString = queryString + "August DECIMAL NULL, "
queryString = queryString + "September DECIMAL NULL, "
queryString = queryString + "October DECIMAL NULL, "
queryString = queryString + "November DECIMAL NULL, "
queryString = queryString + "December DECIMAL NULL, "
queryString = queryString + "Total DECIMAL NULL, "
queryString = queryString + "Total_Calculated DECIMAL NULL, "
queryString = queryString + "Verify_Calculation int NULL "

queryString = queryString + ") ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=utf8mb4_0900_ai_ci;"


queries.append(queryString)
#print(queryString)




# query for entering data into table
for i in range(0,len(arrObjs)):
    rec = arrObjs[i]
    queryString = 'INSERT INTO mrts VALUES ('
    for key in dbkeys:
        value = rec[key]
        queryString += str(value)
        if(key != lastcol): # hard-coded last column... TODO: consider a way to generalize the last key
            queryString += ','
    queryString += (')')
    queries.append(queryString)
    if(i == 4):
        print(queryString)

print(len(queries))
#print(queries[4])
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

query = ("SELECT * FROM mrts")
#cursor.execute(query)

df = pd.read_sql(query, con= db_connection_string)

print(df.head())
print(df.shape)



###################################################################
# close the cursor and db connection
###################################################################

cursor.close()
db_connection_string.close()