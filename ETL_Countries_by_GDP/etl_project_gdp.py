import pandas as pd
import requests as req
from datetime import datetime as dt
import sqlite3

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
csv_file = './Countries_by_GDP.csv'
table_name = 'Countries_by_GDP'
db = 'World_Economies.db'
columns = ['Country','GDP_USD_billion']
log_file = './etl_project_log.txt'
sql_connection = sqlite3.connect(db)

def extact():
    tables = pd.read_html(url)
    gdp_table = tables[3]
    gdp_table.columns = range(8)
    gdp_table.drop(index=[0],inplace=True)
    gdp_table.drop(columns=[1,3,4,5,6,7],inplace=True)
    gdp_table.reset_index(drop=True, inplace=True)
    gdp_table.columns = ['Country','GDP_USD_million']
    return gdp_table

def transform(dataframe):
    dataframe = dataframe[dataframe['GDP_USD_million'] != '—']
    dataframe.reset_index(drop=True, inplace=True)
    dataframe['GDP_USD_million'] = dataframe['GDP_USD_million'].astype(int)
    dataframe['GDP_USD_million'] = dataframe['GDP_USD_million'] / 1000
    dataframe['GDP_USD_million'] = dataframe['GDP_USD_million'].round(2)
    dataframe.columns = ['Country','GDP_USD_billion']
    return dataframe

def load(dataframe):
    dataframe.to_csv(csv_file, index=False)
    dataframe.to_sql(table_name, sql_connection, if_exists='replace', index=False)



def log(massage):
    with open(log_file,'a') as file_log:
        file_log.write(f'{dt.now()} - {massage}\n')

log('ETL process begins')

log('extraction begins')
data = extact()
log('extraction ends')
log('transformation begins')
data = transform(data)
log('transformation ends')
log('loading begins')
load(data)
log('loading ends')

log('ETL process ends')

condition = 'GDP_USD_billion > 100'
print(pd.read_sql(f'select * from {table_name} where {condition}',sql_connection))



sql_connection.close()