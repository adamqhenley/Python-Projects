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
ax[0,0].plot(df_dict['Book']['Sales_Date'],df_dict['Book']['Amount'])
ax[0,0].set_title('Book Stores')

ax[0,1].plot(df_dict['Sporting']['Sales_Date'],df_dict['Sporting']['Amount'])
ax[0,1].set_title('Sporting Goods Stores')

ax[1,0].plot(df_dict['Hobby']['Sales_Date'],df_dict['Hobby']['Amount'])
ax[1,0].set_title('Hobby Toy and Game Stores')

ax[1,1].plot(df_dict['Retail']['Sales_Date'],df_dict['Retail']['Amount'])
ax[1,1].set_title('Retail and Food Services')

#plt.show()

def MakeSeasonalPlots(SalesCategory):
    #
    #
    # Seasonal Plot, ref: https://www.machinelearningplus.com/time-series/time-series-analysis-python/
    #
    df_dict[SalesCategory]['year'] = [d.year for d in df_dict[SalesCategory].Sales_Date]
    df_dict[SalesCategory]['month'] = [d.strftime('%b') for d in df_dict[SalesCategory].Sales_Date]
    years = df_dict[SalesCategory]['year'].unique()
    # Prep Colors
    np.random.seed(100)
    mycolors = np.random.choice(list(mpl.colors.XKCD_COLORS.keys()), len(years), replace=False)
    # Draw Plot
    plt.figure(figsize=(8,6), dpi= 80)
    for i, y in enumerate(years):
        if i > 0:        
            plt.plot('month', 'Amount', data=df_dict[SalesCategory].loc[df_dict[SalesCategory].year==y, :], color=mycolors[i], label=y)
            plt.text(df_dict[SalesCategory].loc[df_dict[SalesCategory].year==y, :].shape[0]-.9, df_dict[SalesCategory].loc[df_dict[SalesCategory].year==y, 'Amount'][-1:].values[0], y, fontsize=12, color=mycolors[i])
    # Decoration
    #plt.gca().set(xlim=(-0.3, 11), ylim=(2, 30), ylabel='$Book Sales$', xlabel='$Month$')
    plt.yticks(fontsize=12, alpha=.7)
    plt.title(f"Seasonal Plot of {SalesCategory} Sales Time Series", fontsize=20)
    #
    #
    # Boxplot of Month-wise (Seasonal) and Year-wise (trend) Distribution
    # Draw Plot
    fig, axes = plt.subplots(1, 2, figsize=(20,7), dpi= 80)
    sns.boxplot(x='year', y='Amount', data=df_dict[SalesCategory], ax=axes[0])
    sns.boxplot(x='month', y='Amount', data=df_dict[SalesCategory].loc[~df_dict[SalesCategory].year.isin([1991, 2008]), :])
    # Set Title
    axes[0].set_title(f'{SalesCategory} Year-wise Box Plot\n(The Trend)', fontsize=18); 
    axes[1].set_title(f'{SalesCategory} Month-wise Box Plot\n(The Seasonality)', fontsize=18)

for k in descr_keys:
    MakeSeasonalPlots(k)


###################################################################
# Seasonal Decomposition
###################################################################



# def MakeSeasonalPlots(SalesCategory):
#     #
#     # Multiplicative Decomposition 
#     result_mul = seasonal_decompose(df_dict[SalesCategory]['Amount'], model='multiplicative', extrapolate_trend='freq')
#     # Additive Decomposition
#     result_add = seasonal_decompose(df_dict[SalesCategory]['Amount'], model='additive', extrapolate_trend='freq')
#     # Plot
#     plt.rcParams.update({'figure.figsize': (10,10)})
#     result_mul.plot().suptitle(f'{SalesCategory} Multiplicative Decompose', fontsize=22)
#     result_add.plot().suptitle(f'{SalesCategory} Additive Decompose', fontsize=22)
#     #
    

# for k in descr_keys:
#     MakeSeasonalPlots(k)






plt.show()

###################################################################
# close the cursor and db connection
###################################################################

cursor.close()
db_connection_string.close()