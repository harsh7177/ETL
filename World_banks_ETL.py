
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def extract(url):
    page=requests.get(url).text
    data=bs(page)
    table=data.find_all('tbody')
    rows=table[0].find_all('tr')
    df=pd.DataFrame()
    for row in rows:
        try:
            bank_name=row.find_all('td')[1].text.strip()
            capital=row.find_all('td')[2].text.strip()
            df_dict={"Bank_Name":bank_name,"Capital":capital}
            df1=pd.DataFrame(df_dict,index=[0])
            df=pd.concat([df,df1],ignore_index=True)
        except:
            pass
    return df

def transform(df):
    
    df['Capital USD']=[cap.replace(',','') for cap in df['Capital']]
    df['Capital GBP']=[round(float(cap.replace(',',''))*0.8,2) for cap in df['Capital']]
    df['Capital INR']=[round(float(cap.replace(',',''))*82.95,2) for cap in df['Capital']]
    df['Capital EUR']=[round(float(cap.replace(',',''))*0.93,2) for cap in df['Capital']]
    return df
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, table_name, conn):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
def run_query(query_statement, conn):
    print(query_statement)
    query_output = pd.read_sql(query_statement, conn)
    print(query_output)
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

url='https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'L_Banks.db'
table_name = 'Largest_Banks'





log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')



conn = sqlite3.connect('L_BANKS.db')
query_output = pd.read_sql(query_statement, conn)

log_progress('SQL Connection initiated.')

load_to_db(df, table_name, conn)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, conn)

log_progress('Process Complete.')

conn.close()
