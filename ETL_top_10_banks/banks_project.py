import pandas as pd
import requests as req
from datetime import datetime as dt
import sqlite3

data_url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv = './exchange_rate.csv'
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
    print(f'extract:\n{MC_table}')
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
    print(f'transform:\n{dataframe}')
    print(f'market capitalization of the 5th largest bank in billion EUR: {dataframe["MC_EUR_Billion"][4]}')
    return dataframe

def load_to_csv(dataframe):
    dataframe.to_csv(csv_file, index=False)

def load_to_db(dataframe):
    dataframe.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_queries():
    print(pd.read_sql(f'select * from {table_name}',sql_connection))
    print(pd.read_sql(f'SELECT AVG(MC_GBP_Billion) FROM {table_name}',sql_connection))
    print(pd.read_sql(f'SELECT Name from {table_name} LIMIT 5',sql_connection))



def log_progress(massage):
    with open(log_file,'a') as file_log:
        file_log.write(f'{dt.now()} : {massage}\n')

log_progress('ETL process begins')

log_progress('extraction begins')
data = extact()
log_progress('extraction ends')
log_progress('transformation begins')
data = transform(data)
log_progress('transformation ends')
log_progress('loading begins')
load_to_csv(data)
load_to_db(data)
log_progress('loading ends')

log_progress('ETL process ends')

run_queries()



sql_connection.close()