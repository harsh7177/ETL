#!/usr/bin/env python
# coding: utf-8

# In[8]:


from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def extract(product):
    url='https://www.flipkart.com/search?q={}'.format(self.product)
    headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.3; Win 64 ; x64) Apple WeKit /537.36(KHTML , like Gecko) Chrome/80.0.3987.162 Safari/537.36'}
    scrap=requests.get(url,headers=headers)
    web=bs(scrap.text)
    page_num=int(web.find_all('div',class_="_2MImiq")[0].text.split('1234')[0][-3::1])
    Product=[]
    price=[]
    Discount=[]
    Review=[]
    Rating=[]
    for i in range(page_num):
        n='https://www.flipkart.com/search?q={}&page={}'.format(product,i)
        n_scrap=requests.get(n,headers=headers).text
        bs_s=bs(n_scrap)
    for i in  bs_s.find_all('div',class_="_4rR01T"):
        Product.append(i.text)
    for i in bs_s.find_all('div',class_='_30jeq3 _1_WHN1'):
        price.append(int(i.text.replace(',','').replace('₹','')))
    for i in bs_s.find_all('span',class_="_2_R_DZ"):
        Review.append(i.text.split('\xa0&\xa0'))
    for i in bs_s.find_all('div',class_="_3LWZlK"):
        Rating.append(i.text[0])
df=pd.DataFrame(list(zip(Product,price,Review,Rating)),columns=['Name','Price','Rating&Reviews','Rating'])

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


# In[3]:


db_name = 'flipkart_reviews.db'
table_name = 'Reviews'


# In[ ]:


product=input("Enter the product name:- ")

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(product)

conn = sqlite3.connect('flipkart_reviews.db')
query_output = pd.read_sql(query_statement, conn)

log_progress('SQL Connection initiated.')

load_to_db(df, table_name, conn)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, conn)

log_progress('Process Complete.')

conn.close()

