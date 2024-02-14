# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3

def log_progress(message):
    time_format = "%Y-%m-%d %H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(time_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")

def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    html_txt = requests.get(url).text
    soup = BeautifulSoup(html_txt, 'html.parser')
    table = soup.find_all('tbody')[0]
    rows = table.find_all('tr')
    for row in rows:
        tds = row.find_all('td')
        if len(tds)>0:
            data_dict = {
                table_attribs[0]: tds[1].find_all('a')[1].contents[0],
                table_attribs[1]: float(tds[2].contents[0].replace('\n',''))
            }
            df = pd.concat([df, pd.DataFrame(data_dict,index=[0])],
                            ignore_index=True)
    return df

def transform(df, csv_path):
    ex_df = pd.read_csv(csv_path)
    ex_dc = ex_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*ex_dc['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*ex_dc['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*ex_dc['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

URL = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_path = './exchange_rate.csv'
csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']

log_progress('Preliminaries complete')
log_progress('Initiating ETL process')
df = extract(URL, table_attribs)
#print(df)
log_progress('Data extraction complete')
log_progress('Initiating Transformation process')
df = transform(df, exchange_path)
#print(df)
log_progress('Data transformation complete')
log_progress('Initiating Loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
conn = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')
load_to_db(df,conn, table_name)
log_progress('Data loaded to Database as a table, Executing queries')
qry_statement = 'SELECT * FROM Largest_banks'
run_query(qry_statement, conn)
qry_statement = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(qry_statement, conn)
qry_statement = 'SELECT Name from Largest_banks LIMIT 5'
run_query(qry_statement, conn)
log_progress('Process Complete')
conn.close()
log_progress('Server Connection closed')
