import mysql.connector
import numpy as np
import pandas as pd
import yaml
import matplotlib.pyplot as plt
from dateutil.parser import parse 
import matplotlib as mpl
import seaborn as sns
from statsmodels.tsa.seasonal import seasonal_decompose
from dateutil.parser import parse


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

descriptions = {}
descriptions['Retail'] = 'Retail and food services sales% total'
descriptions['Book'] = 'Book stores'
descriptions['Hobby'] = 'Hobby% toy% and game stores'
descriptions['Sporting'] = 'Sporting goods stores'

descr_keys = list(descriptions.keys())
#print(descr_keys)
queries = []
for descr in descriptions:
    query = (f"""SELECT Sales_Date, Amount
            FROM mrts_monthly
            WHERE Description = '{descriptions[descr]}'
            """)
    queries.append(query)
#print(queries)

df_dict = {}
for i in range(0,len(queries)):
    dfName = descr_keys[i]
    df_dict[dfName] = pd.read_sql(queries[i], con= db_connection_string)
    print(f'\n\n{dfName}:\n')
    print(df_dict[dfName].head())
    print(type(df_dict[dfName]['Sales_Date'].iloc[0]))
    df_dict[dfName] = df_dict[dfName].loc[df_dict[dfName]['Sales_Date'] <= '2021.02.01']
    df_dict[dfName]['Sales_Date'] = pd.to_datetime(df_dict[dfName]['Sales_Date'])
    df_dict[dfName] = df_dict[dfName].sort_values(by='Sales_Date')
    




###################################################################
# Output
###################################################################

# 4 subplot demo, ref: https://matplotlib.org/stable/gallery/subplots_axes_and_figures/subplots_demo.html

fig, ax = plt.subplots(2,2)
ax[0,0].plot(df_dict['Book']['Sales_Date'],df_dict['Book']['Amount'].rolling(24).mean())
ax[0,0].set_title('Book Stores')

ax[0,1].plot(df_dict['Sporting']['Sales_Date'],df_dict['Sporting']['Amount'].rolling(24).mean())
ax[0,1].set_title('Sporting Goods Stores')

ax[1,0].plot(df_dict['Hobby']['Sales_Date'],df_dict['Hobby']['Amount'].rolling(24).mean())
ax[1,0].set_title('Hobby Toy and Game Stores')

ax[1,1].plot(df_dict['Retail']['Sales_Date'],df_dict['Retail']['Amount'].rolling(24).mean())
ax[1,1].set_title('Retail and Food Services')

plt.show()



###################################################################
# close the cursor and db connection
###################################################################

cursor.close()
db_connection_string.close()