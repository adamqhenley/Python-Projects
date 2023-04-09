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
# Input
###################################################################

use_mrts_db_sql = 'USE mrts_db;'
cursor.execute(use_mrts_db_sql);

###################################################################
# begin analysis
###################################################################

# mrts monthly data (after further pre-processing)
query = ("""SELECT Sales_Date, Amount
            FROM mrts_monthly
            WHERE Description = 'Retail and food services sales% total'
            """)

df_mrts_monthly_from_db = pd.read_sql(query, con= db_connection_string)

print(df_mrts_monthly_from_db.head())
print(df_mrts_monthly_from_db.shape)
pd.to_datetime(df_mrts_monthly_from_db['Sales_Date'])

df_mrts_monthly_from_db.sort_values(by='Sales_Date',inplace=True)



###################################################################
# Output
###################################################################

plt.plot(df_mrts_monthly_from_db['Sales_Date'],df_mrts_monthly_from_db['Amount'])
plt.show()



###################################################################
# close the cursor and db connection
###################################################################

cursor.close()
db_connection_string.close()