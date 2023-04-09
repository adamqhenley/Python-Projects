import mysql.connector
import pandas as pd
import yaml
import matplotlib.pyplot as plt


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
print(df_mrts.head())
print(df_mrts.shape)


arrObjs = []
for i in range(0,len(df_mrts)):
    newrowObj = {}
    for c in df_mrts.columns:
        if(c == 'Description'):
            newrowObj[c] = f'"{df_mrts[c].loc[i]}"'
        else:
            newrowObj[c] = df_mrts[c].loc[i]
    arrObjs.append(newrowObj)

print(arrObjs[0])

dbkeys = list(arrObjs[0].keys())
lastcol = dbkeys[-1]


###################################################################
# setup queries
###################################################################

queries = []

q1 = "DROP DATABASE IF EXISTS `mrts_db`;"
q2 = "CREATE DATABASE IF NOT EXISTS `mrts_db`;"
q3 = "USE `mrts_db`;"

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



# query for entering data into table (mrts)
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


#query for creating table mrts_monthly
queryString = "CREATE TABLE mrts_monthly ("
queryString = queryString + "ID int NOT NULL, "
queryString = queryString + "NAICS_Code_1 int NULL, "
queryString = queryString + "NAICS_Code_2 int NULL, "
queryString = queryString + "NAICS_Code_3 int NULL, "
queryString = queryString + "Description varchar (200) NULL, "
queryString = queryString + "Adjusted int NULL, "
queryString = queryString + "Sales_Date varchar (10) NULL, "
queryString = queryString + "Amount int NULL "
queryString = queryString + ") ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=utf8mb4_0900_ai_ci;"
queries.append(queryString)

monthsdict = {'Jan':'01',
              'Feb':'02',
              'Mar':'03',
              'Apr':'04',
              'May':'05',
              'Jun':'06',
              'Jul':'07',
              'Aug':'08',
              'Sep':'09',
              'Oct':'10',
              'Nov':'11',
              'Dec':'12'}

# create date values from month values
monthObjArr = []
newidset = 1
for i in range(0,len(arrObjs)):
    obj = arrObjs[i]
    year = obj['Year']
    for m in monthsdict:
        newObj = {}
        newObj['ID'] = newidset
        newObj['NAICS_Code_1'] = obj['NAICS_Code_1']
        newObj['NAICS_Code_2'] = obj['NAICS_Code_2']
        newObj['NAICS_Code_3'] = obj['NAICS_Code_3']
        newObj['Description'] = obj['Description']
        newObj['Adjusted'] = obj['Adjusted']
        monthNum = monthsdict[m]
        datestr = f'"{year}.{monthNum}.01"'
        newObj['Sales_Date'] = datestr
        newObj['Amount'] = obj[m]
        monthObjArr.append(newObj)
        newidset = newidset + 1
    
dbkeys = list(monthObjArr[0].keys())
lastcol = dbkeys[-1]

    #month = monthsdict[year]
# query for entering data into table (mrts)
for i in range(0,len(monthObjArr)):
    rec = monthObjArr[i]
    queryString = 'INSERT INTO mrts_monthly VALUES ('
    for key in dbkeys:
        value = rec[key]
        queryString += str(value)
        if(key != lastcol): # hard-coded last column... TODO: consider a way to generalize the last key
            queryString += ','
    queryString += (')')
    queries.append(queryString)
    if(i == 4):
        print(queryString)


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

# mrts all data (raw)
query = ("SELECT * FROM mrts")

df_mrts_from_db = pd.read_sql(query, con= db_connection_string)

print(df_mrts_from_db.head())
print(df_mrts_from_db.shape)

# mrts monthly data (after further pre-processing)
query = ("SELECT Sales_Date, Amount FROM mrts_monthly")

df_mrts_monthly_from_db = pd.read_sql(query, con= db_connection_string)

print(df_mrts_monthly_from_db.head())
print(df_mrts_monthly_from_db.shape)


###################################################################
# close the cursor and db connection
###################################################################

cursor.close()
db_connection_string.close()