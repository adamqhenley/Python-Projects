import mysql.connector
import pandas as pd
import yaml


dbinfo = yaml.safe_load(open('db.yaml'))
config = {
    'user':             dbinfo['user'],
    'password':         dbinfo['pwrd'],
    'host':             dbinfo['host'],
    'database':         dbinfo['db'],
    #'auth_plugin':      'mysql_native_password'
}

db_connection_string = mysql.connector.connect(**config)


query = 'SELECT * FROM colleges'

df = pd.read_sql(query, con= db_connection_string)

print(df)