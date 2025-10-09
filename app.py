'''
Author             : Bhavani Kishore
Date               : 30/09/2025
Last Modified date : 05/10/2025

Description:
    This app.py is for implementing all the logic it import all tables.py and dq.py and config.json
    and implement the logics hear
    if you want to run this proggram then run Python3 app.py
    This project builds an ELT pipeline for currency exchange data, extracting rates from API.
    Raw data is loaded into a CSV for centralized storage and scalability.
    Transformations clean, standardize, and compute insights like trends and cross-rates.
    The pipeline powers dashboards and analytics for monitoring, forecasting, and decision-making for Checking Currency rates.
'''

# Codes and url with api key for checking
# USD,EUR,JPY,GBP,AUD,NZD,CAD,CHF,CNY,CNH,HKD,SGD,INR,KRW,TWD,THB,IDR,MYR,PHP,VND,BRL,MXN,CLP,COP,ARS,RUB,ZAR,TRY,AED,SAR,QAR,KWD,BHD,OMR,ILS
# https://api.currencylayer.com/timeframe?access_key=d175ce1f52350bfdc5d95afbf408c6b4&currencies=USD,EUR,JPY,GBP,AUD,NZD,CAD,CHF,CNY,CNH,HKD,SGD,INR,KRW,TWD,THB,IDR,MYR,PHP,VND,BRL,MXN,CLP,COP,ARS,RUB,ZAR,TRY,AED,SAR,QAR,KWD,BHD,OMR,ILS&start_date=2025-01-01&end_date=2025-10-29


#importing required libraries
import pandas as pd
import requests
import json
import time
import os
import tables as tb
import dq
import datetime


#----Importing Configuration file --------

with open("./config/config.json","r") as fs:
    config=json.load(fs)

codes=config["codes"]["codes"]
currencies=""
for i in codes:
    currencies+=i+","
currencies=currencies[:-1]
#----Extraction Historical Data and Storing into Json & csv--------------
if(bool(config["torun"]["featch_hist"])):
    res=requests.get(f"https://api.currencylayer.com/timeframe?access_key={config["urls"]["access_key"]}&currencies={currencies}&start_date={config["urls"]["start_date"]}&end_date={config["urls"]["end_date"]}") #To Fetch Historical Data
    res=res.json() # converting loaded res into into 
    df=pd.DataFrame(res)
    file_name=time.strftime("hist")
    dir_name=f"{config['path']['hist']}"
    os.makedirs(dir_name, exist_ok=True) #making Dir For Historical File
    df.to_json(dir_name+f"{file_name}.json") #Saving json file
    df.to_csv(dir_name+f"{file_name}.csv") #Saving csv File
    time.sleep(3)

#---- Loding Historical Data and Inserting it into DB ----------

if(bool(config["torun"]["load_hist"])):
    with open(config["path"]['hist']+"hist.json",'r') as fs:
        data=json.load(fs)
    data_df=data["quotes"] #dict
    df=pd.DataFrame.from_dict(data_df,orient='index')
    df.reset_index(names="date") #DataFrame
    df.to_csv('tocheck.csv',index=[0])
    tb.create_table("data") #creating table  With name data
    tb.insert_hist_data(df,"data") # inserting historical data into table data


#----Extraction Live Data and Storing into Json & csv--------------

if(bool(config["torun"]["featch_live"])):
    res=requests.get(f"https://api.currencylayer.com/live?access_key={config["urls"]["access_key"]}&currencies={currencies}").json() #To Fetch Historical Data
    file_name=time.strftime("%d_%b_%Y_%H:%M:%S")
    dir_name=f"{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}"
    df=pd.DataFrame(res)
    os.makedirs("./dist/json/"+dir_name, exist_ok=True) #Creating Dir for json file to store if not present
    df.to_json("./dist/json/"+f"{dir_name}/"+f"{file_name}.json")
    os.makedirs("./dist/csv/"+dir_name, exist_ok=True) #Creating Dir for csv file to store if not present
    df.to_csv("./dist/csv/"+f"{dir_name}/"+f"{file_name}.csv")
    time.sleep(3) #waiting 1 hr


#----Using All Files in Todays Folder updating it in db-------------

if(bool(config["torun"]["load_live"])):
    dir_name=f"{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}"
    all_files=list(os.listdir(config["dist"]["csv"]+f'{dir_name}'))
    for file in all_files: #checking data quality rules and inserting in database
        file_name=file
        file=config["dist"]["csv"]+f'{dir_name}/{file}'
        df=pd.read_csv(file, index_col=[0])
        df=dq.applying_dq_rules_live(df,file_name) #apply all dq rules on the df
        tb.create_table("data")
        tb.insert_live_data(df,"data") #then insert that data into table 



# codes=config["codes"]["codes"]
# currencies=""
# for i in codes:
#     currencies+=i+","
# currencies=currencies[:-1]
# url=f"https://api.currencylayer.com/timeframe?access_key={config["urls"]["access_key"]}&currencies={currencies}&start_date={config["urls"]["start_date"]}&end_date={config["urls"]["end_date"]}",
# print(url[2:len(url)-2])
    





#orchestrationch

# while(True):
#     t=3
#     while(t):
#         t-=1
#         res=requests.get(config["urls"]["live"]).json() #To Fetch live Data
#         file_name=time.strftime("%d_%b_%Y_%H:%M:%S")
#         dir_name=f"{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}"
#         df=pd.DataFrame(res)
#         os.makedirs("./dist/json/"+dir_name, exist_ok=True) #Creating Dir for json file to store if not present
#         df.to_json("./dist/json/"+f"{dir_name}/"+f"{file_name}.json")
#         os.makedirs("./dist/csv/"+dir_name, exist_ok=True) #Creating Dir for csv file to store if not present
#         df.to_csv("./dist/csv/"+f"{dir_name}/"+f"{file_name}.csv")
#         time.sleep(10) #waiting 1 hr

#     dir_name=f"{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}"
#     all_files=list(os.listdir(config["dist"]["csv"]+f'{dir_name}'))
#     for file in all_files: #checking data quality rules and inserting in database
#         file_name=file
#         file=config["dist"]["csv"]+f'{dir_name}/{file}'
#         df=pd.read_csv(file, index_col=[0])
#         df=dq.applying_dq_rules(df,file_name) #apply all dq rules on the df
#         tb.insert_live_data(df,"data") #then insert that data into table 

