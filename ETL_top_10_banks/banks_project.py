import pandas as pd
import requests as req
from datetime import datetime as dt
import sqlite3

data_url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
csv_file = './Largest_banks_data.csv'
table_name = 'Largest_banks'
db = 'Banks.db'
columns = ['Name','MC_USD_Billion']
log_file = './code_log.txt'
sql_connection = sqlite3.connect(db)

def extact():
    tables = pd.read_html(data_url)
    MC_table = tables[1]
    MC_table.drop(columns=['Rank'],inplace=True)
    MC_table.columns = columns
    return MC_table

def transform(dataframe):
    dataframe['MC_USD_Billion'] = dataframe['MC_USD_Billion'].astype(float)
    exchange_rate = pd.read_csv(exchange_rate_csv)
    dict_records = exchange_rate.to_dict(orient='list')
    Currency = dict_records['Currency']
    Rate = dict_records['Rate']
    for currency,rate in zip(Currency,Rate):
        dataframe[f'MC_{currency}_Billion'] = dataframe['MC_USD_Billion'] * rate
        dataframe[f'MC_{currency}_Billion'] = dataframe[f'MC_{currency}_Billion'].round(2)
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

print(pd.read_sql(f'select * from {table_name}',sql_connection))



sql_connection.close()