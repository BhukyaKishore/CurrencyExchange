'''
Author : Bhavani Kishore
Date : 30/10/2025
url="https://api.currencylayer.com/timeframe?access_key=d175ce1f52350bfdc5d95afbf408c6b4&start_date=2025-01-01&end_date=2025-10-29" for fetching data

Description:
    This page is to Establish database Connection and doing data insertion functions in this code
'''

import mysql.connector
import json 
import pandas as pd
import os 
import time 
import datetime

#Featching Config files
with open("config.json",'r') as fs:
    config=json.load(fs)

#Declearing Global Variables
mycursor=None
mydb=None

# Function to Conect Database
def db_connect():
  global mycursor,mydb
  try:
    mydb = mysql.connector.connect(
      host=config["database"]["host"],
      user=config["database"]["user"],
      password=config["database"]["password"]
    )
    mycursor = mydb.cursor()
    mycursor.execute("create database if not exists elt3")
    mycursor.execute("use elt3")
    mydb.commit()
    return mycursor,mydb
  except Exception as err:
     print(err)


# Function to To create database Dynamically
def create_table(df :pd.DataFrame,table_name :str):
    db_connect()
    stm="(dt TIMESTAMP UNIQUE,id INT AUTO_INCREMENT PRIMARY KEY,"
    for column in df.columns:
       stm+=f"{column} FLOAT,"
       # Adding Who Columns To Database
       stm2=F'''
                created_by INT DEFAULT {800355},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated_by INT DEFAULT {800355},
                last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                last_update_login INT DEFAULT {800355});
            '''
    # stm=stm[:-1]
    mycursor,mydb=db_connect()
    mycursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {stm} {stm2}")
    # print(f"CREATE TABLE IF NOT EXISTS {table_name} {stm} {stm2}")
    mydb.commit()
   

# Function to To insert Hisorical Data into database Dynamically
def insert_hist_data(df :pd.DataFrame,table_name:str):
  db_connect()
  stm=f"insert ignore into {table_name}  (dt ,"
  for column in df.columns:
     stm+=f"{column},"
  stm=stm[:-1]+") values "
  inser_cmd=""
  for index, row in df.iterrows():
    inser_cmd+=f"('{index} 00:00:00',"
    for column in df.columns:
      inser_cmd+=f"{row[column]},"
    inser_cmd=inser_cmd[:-1]+"),\n"
  inser_cmd=inser_cmd[:-2]+";"
  mycursor,mydb=db_connect()
  mycursor.execute(stm+inser_cmd)
  mydb.commit()



#------Function to insert live data----------

def insert_live_data(df :pd.DataFrame,table_name:str):
  db_connect()
  date=pd.to_datetime(df.loc[["USDSAR"],'timestamp']+19800,unit='s')["USDSAR"]
  cols=f"(dt ,"
  values=f'("{date} ",'
  for index,row in df.iterrows():
    cols+=f"{index} ,"
    values+=f"{row["quotes"]} ,"
  cols=cols[:-1]+") "
  values=values[:-1]+");"
  stm=f"INSERT IGNORE INTO {table_name} {cols} values {values}"
  mycursor,mydb=db_connect()
  mycursor.execute(stm)
  mydb.commit()
  

df=pd.read_csv('tocheck.csv')
insert_live_data(df,"data")



