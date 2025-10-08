''''
Author : Bhavani Kishore
Date : 01/10/2025

description:
    dq.py handles data quality checks before loading data into the database.
    It validates columns for nulls.
    Invalid rows are logged and dropped to ensure clean, consistent data.
'''

#importing required libraries
import time
import pandas as pd
import json
import os
import numpy as np
import datetime


#Reading Config.json  File
with open("config.json","r") as fs:
    config=json.load(fs)
def creating_log_and_error_file(file_name:str):
    os.makedirs(config["dist"]["log"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}', exist_ok=True)
    # Log any nulls found on file 
    with open(config["dist"]["log"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}'+f"/{file_name}.log", 'w') as fs:
        fs.write(f'''
                 
Project:    Currency Exchange (ELT) 
Author :    Bhavani Kishore
Created_on: {time.strftime("%a, %d %b %Y %H:%M:%S")}
VERSION     :1.0
------------------------------------------------------------------------------------------------------------

                ''')
    #Making Dir for Error file with current date 
    os.makedirs(config["dist"]["error"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}', exist_ok=True)
    # Writing Error in Error File
    with open(config["dist"]["error"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}'+f"/{file_name}.log", 'w') as fs:
        fs.write(f'''
 
Project:    Currency Exchange (ELT) 
Author :    Bhavani Kishore
Created_on: {time.strftime("%a, %d %b %Y %H:%M:%S")}
VERSION     :1.0
------------------------------------------------------------------------------------------------------------
                ''')



def bool_check_and_drop(df :pd.DataFrame ,col :str , file_name :str):
    bools = df[col].astype(str).str.capitalize()
    is_valid = bools.isin(['True'])
    invalid_index = df[~is_valid].index.tolist()
    #making dir for logging file with current date 
    os.makedirs(config["dist"]["log"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}', exist_ok=True)
    # Log any nulls found on file 
    with open(config["dist"]["log"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}'+f"/{file_name}.log", 'a') as fs:
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
        for idx in invalid_index:
            fs.write(f'''
At {timestamp} time -> boolean value error in table in column "{col}" at code  {idx} in filie {file_name}
------------------------------------------------------------------------------------------------------------

''')
    #Making Dir for Error file with current date 
    os.makedirs(config["dist"]["error"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}', exist_ok=True)
    # Writing Error in Error File
    with open(config["dist"]["error"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}'+f"/{file_name}.log", 'a') as fs:
        for idx in invalid_index:
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
            fs.write(f'''Boolean value  Error in file {file_name} on time {timestamp} -> {df.loc[idx]}\n\n\n\n
------------------------------------------------------------------------------------------------------------

''')
    return df[is_valid].copy()    


def terms_url_check(df :pd.DataFrame ,col :str , file_name :str):
    pattern=r'https?://(?:www\.)?\S+|www\.\S+'
    is_valid=df[col].astype(str).str.match(pattern,na=False)
    invalid_index = df[~is_valid].index.tolist()
    #making dir for logging file with current date 
    os.makedirs(config["dist"]["log"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}', exist_ok=True)
    # Log any nulls found on file 
    with open(config["dist"]["log"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}'+f"/{file_name}.log", 'a') as fs:
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
        for idx in invalid_index:
            fs.write(f'''At {timestamp} time -> terms url is not a url  in column "{col}" at code  {idx} in filie {file_name} so, it is updated by default value "https://currencylayer.com/terms" 
------------------------------------------------------------------------------------------------------------

''')
    #Making Dir for Error file with current date 
    os.makedirs(config["dist"]["error"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}', exist_ok=True)
    # Writing Error in Error File
    with open(config["dist"]["error"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}'+f"/{file_name}.log", 'a') as fs:
        for idx in invalid_index:
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
            fs.write(f'''Terms url Error in file {file_name} on time {timestamp} -> {df.loc[idx]}
------------------------------------------------------------------------------------------------------------

''')
            df.loc[idx,col]='https://currencylayer.com/terms'
    return df


def privacy_url_check(df :pd.DataFrame ,col :str , file_name :str):
    pattern=r'https?://(?:www\.)?\S+|www\.\S+'
    is_valid=df[col].astype(str).str.match(pattern,na=False)
    invalid_index = df[~is_valid].index.tolist()
    #making dir for logging file with current date 
    os.makedirs(config["dist"]["log"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}', exist_ok=True)
    # Log any nulls found on file 
    with open(config["dist"]["log"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}'+f"/{file_name}.log", 'a') as fs:
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
        for idx in invalid_index:
            fs.write(f'''At {timestamp} time -> privacy url is not a url  in column "{col}" at code  {idx} in filie {file_name} so, it is updated by default value "https://currencylayer.com/privacy" 
------------------------------------------------------------------------------------------------------------

''')
    #Making Dir for Error file with current date 
    os.makedirs(config["dist"]["error"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}', exist_ok=True)
    # Writing Error in Error File
    with open(config["dist"]["error"]+f'{str(time.strftime("%Y"))}/{str(time.strftime("%b"))}/{str(time.strftime("%d"))}'+f"/{file_name}.log", 'a') as fs:
        for idx in invalid_index:
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
            fs.write(f'''Privacy value  Error in file {file_name} on time {timestamp} -> {df.loc[idx]}
------------------------------------------------------------------------------------------------------------

''')
            df.loc[idx,col]='https://currencylayer.com/privacy'
    return df


def timestamp_check_and_drop(df: pd.DataFrame, col: str, file_name: str) -> pd.DataFrame:
    # Apply validation
    is_valid =df[col].apply(lambda x: isinstance(x, int))
    invalid_index = df[~is_valid].index.tolist()

    # Logging setup
    log_dir = config["dist"]["log"] + f'{time.strftime("%Y")}/{time.strftime("%b")}/{time.strftime("%d")}'
    error_dir = config["dist"]["error"] + f'{time.strftime("%Y")}/{time.strftime("%b")}/{time.strftime("%d")}'
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(error_dir, exist_ok=True)
    # Log file
    with open(f"{log_dir}/{file_name}.log", "a") as fs:
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
        for idx in invalid_index:
            fs.write(f'''At {timestamp} time -> invalid timestamp in column "{col}" at code  {idx} in file {file_name}
------------------------------------------------------------------------------------------------------------

''')

    # Error file
    with open(f"{error_dir}/{file_name}.log", "a") as fs:
        for idx in invalid_index:
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
            fs.write(f''' Timestamp Error in file {file_name} on time {timestamp} -> {df.loc[idx]}
------------------------------------------------------------------------------------------------------------

''')

    # Return only valid rows
    return df[is_valid].copy()



def source_check_and_drop(df: pd.DataFrame, col: str, file_name: str) -> pd.DataFrame:
    # Check which rows are valid
    is_valid = df[col].isin(["USD"]) #checking
    invalid_index = df[~is_valid].index.tolist()

    # Logging setup
    log_dir = config["dist"]["log"] + f'{time.strftime("%Y")}/{time.strftime("%b")}/{time.strftime("%d")}'
    error_dir = config["dist"]["error"] + f'{time.strftime("%Y")}/{time.strftime("%b")}/{time.strftime("%d")}'
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(error_dir, exist_ok=True)

    # Log file
    with open(f"{log_dir}/{file_name}.log", "a") as fs:
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
        for idx in invalid_index:
            fs.write(f'''At {timestamp} time -> invalid source in column "{col}" at code  {idx} in file {file_name}
------------------------------------------------------------------------------------------------------------

''')

    # Error file
    with open(f"{error_dir}/{file_name}.log", "a") as fs:
        for idx in invalid_index:
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
            fs.write(f'''Source Error in file {file_name} on time {timestamp} -> {df.loc[idx]}
------------------------------------------------------------------------------------------------------------

''')
    return df[is_valid].copy()



def code_check_and_drop(df: pd.DataFrame, col: str, file_name: str) -> pd.DataFrame:
    # List of valid codes
    valid_codes = [
        "USD","EUR","JPY","GBP","AUD","NZD","CAD","CHF","CNY","CNH","HKD","SGD","INR","KRW","TWD","THB",
        "IDR","MYR","PHP","VND","BRL","MXN","CLP","COP","ARS","RUB","ZAR","TRY","AED","SAR","QAR","KWD",
        "BHD","OMR","ILS"
    ]
    
    # Convert column to string and check conditions
    codes = df[col].astype(str)
    is_valid = codes.apply(lambda x: isinstance(x, str) and len(x) == 3 and x in valid_codes)
    invalid_index = df[~is_valid].index.tolist()

    # Logging setup
    log_dir = config["dist"]["log"] + f'{time.strftime("%Y")}/{time.strftime("%b")}/{time.strftime("%d")}'
    error_dir = config["dist"]["error"] + f'{time.strftime("%Y")}/{time.strftime("%b")}/{time.strftime("%d")}'
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(error_dir, exist_ok=True)

    # Log file
    with open(f"{log_dir}/{file_name}.log", "a") as fs:
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
        for idx in invalid_index:
            fs.write(f'''At {timestamp} time -> invalid code in column "{col}" at code  {idx} in file {file_name}
------------------------------------------------------------------------------------------------------------

''')

    # Error file
    with open(f"{error_dir}/{file_name}.log", "a") as fs:
        for idx in invalid_index:
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
            fs.write(f'''Code Error in file {file_name} on time {timestamp} -> {df.loc[idx]} 
------------------------------------------------------------------------------------------------------------

''')

    # Drop invalid rows
    return df[is_valid].copy()

def rate_check_and_update(df: pd.DataFrame, col: str, file_name: str) -> pd.DataFrame:
    # Check if column exists
    if col not in df.columns:
        raise ValueError(f"Column '{col}' does not exist in the DataFrame.")
    # Validation: must be float & > 0
    def is_valid_rate(x):
        try:
            val = float(x)
            return val > 0
        except:
            return False
    is_valid = df[col].apply(is_valid_rate)
    invalid_index = df[~is_valid].index.tolist()
    #Logging setup
    log_dir = config["dist"]["log"] + f'{time.strftime("%Y")}/{time.strftime("%b")}/{time.strftime("%d")}'
    error_dir = config["dist"]["error"] + f'{time.strftime("%Y")}/{time.strftime("%b")}/{time.strftime("%d")}'
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(error_dir, exist_ok=True)
    with open(f"{log_dir}/{file_name}.log", "a") as fs:
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
        for idx in invalid_index:
            fs.write(f'''At {timestamp} time -> invalid rate in column "{col}" at code  {idx} in file {file_name}
------------------------------------------------------------------------------------------------------------

''')

    # Error file
    with open(f"{error_dir}/{file_name}.log", "a") as fs:
        for idx in invalid_index:
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
            fs.write(f'''Rate Error in file {file_name} on time {timestamp} -> {df.loc[idx]}
------------------------------------------------------------------------------------------------------------

''')
    # Update invalid values with NaN
    df.loc[~is_valid, col] = np.nan
    return df

def code_check(df:pd.DataFrame,file_name:str):
    org_idx=['USDEUR', 'USDJPY', 'USDGBP', 'USDAUD', 'USDNZD', 'USDCAD', 'USDCHF', 'USDCNY', 'USDCNH', 'USDHKD', 'USDSGD', 'USDINR', 'USDKRW', 'USDTWD', 'USDTHB', 'USDIDR', 'USDMYR', 'USDPHP', 'USDVND', 'USDBRL', 'USDMXN', 'USDCLP', 'USDCOP', 'USDARS', 'USDRUB', 'USDZAR', 'USDTRY', 'USDAED', 'USDSAR', 'USDQAR', 'USDKWD', 'USDBHD', 'USDOMR', 'USDILS']
    # df.drop(np.nan, inplace=True)
    idx=df.index.to_list()
    invalid=[]
    for id in idx:
        if id not in org_idx:
            df.drop(id, inplace=True)
    log_dir = config["dist"]["log"] + f'{time.strftime("%Y")}/{time.strftime("%b")}/{time.strftime("%d")}'
    error_dir = config["dist"]["error"] + f'{time.strftime("%Y")}/{time.strftime("%b")}/{time.strftime("%d")}'
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(error_dir, exist_ok=True)
    for id in invalid:
        with open(f"{log_dir}/{file_name}.log", "a") as fs:
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
            fs.write(f'''At {timestamp} time -> invalid index(CODE) at code  {id} in file {file_name}
    ------------------------------------------------------------------------------------------------------------
    ''')
        # Error file
        with open(f"{error_dir}/{file_name}.log", "a") as fs:
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
            fs.write(f'''At {timestamp} time -> invalid index(CODE) at code  {id} in file {file_name}
    ------------------------------------------------------------------------------------------------------------
    ''')
    return df


# Null check for quotes from Csv File
try:
    def applying_dq_rules_live(df: pd.DataFrame,file_name :str) :
        '''check all dq rules for the live data'''
        creating_log_and_error_file(file_name)
        df=code_check(df,file_name)
        # df=bool_check_and_drop(df,"success",file_name)
        # df=terms_url_check(df,"terms",file_name)
        # df=privacy_url_check(df,"privacy",file_name)
        # df=timestamp_check_and_drop(df,"timestamp",file_name)
        # df=source_check_and_drop(df,"source",file_name)
        # df=rate_check_and_update(df,"quotes",file_name)
        df.to_csv('tocheck.csv',index=False)
        return df
except Exception as err:
    with open(f'error.log') as fs:
        fs.write(f'{time.strptime(datetime.datetime.now())} error is {err} at applying_dq_rules_live() function ')

# df=pd.read_csv("./dist/csv/2025/Oct/05/05_Oct_2025_21:10:15.csv",index_col=[0])
# applying_dq_rules(df,"05_Oct_2025_21:10:15.csv")


#-----------------DQ RULES FOR HISTORICAL DATA---------------------

def col_check(df: pd.DataFrame,filename:str):
    '''
        this function will check is weather columns are valid or not 
        if any new column is found than it will be drop that entire column
    '''
    valid_col=["Unnamed: 0","USDEUR","USDJPY","USDGBP","USDAUD","USDNZD","USDCAD","USDCHF","USDCNY","USDCNH","USDHKD","USDSGD","USDINR","USDKRW","USDTWD","USDTHB","USDIDR","USDMYR","USDPHP","USDVND","USDBRL","USDMXN","USDCLP","USDCOP","USDARS","USDRUB","USDZAR","USDTRY","USDAED","USDSAR","USDQAR","USDKWD","USDBHD","USDOMR","USDILS"]
    drop_col=[]
    for col in df.columns:
        if col not in valid_col:
            drop_col.append(col)
    for i in drop_col:
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
        with open('./dist/historical/hist.log','a') as fs:
            fs.write(f'''At {timestamp} time -> Invalid column is found on file {filename} so column name as {i} is droped
------------------------------------------------------------------------------------------------------------

''')
        df.drop(i, axis=1,inplace=True) 
    return df


def date_check(df: pd.DataFrame, file_name: str):
    """
    Check date format in the index and drop invalid rows without losing data.
    Assumes the index contains date strings in YYYY-MM-DD format.
    """
    # Ensure the index is string type
    index_str = df.index.astype(str)
    valid_rows = []
    invalid_dates = []

    for date_str in df.index:
        try:
            # Check correct date format
            dt = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            # Check if date <= today
            if dt <= datetime.date.today():
                valid_rows.append(True)
            else:
                valid_rows.append(False)
                invalid_dates.append(date_str)
        except Exception:
            valid_rows.append(False)
            invalid_dates.append(date_str)
    
    # Convert to boolean Series aligned with df.index
    is_valid = pd.Series(valid_rows, index=df.index)
    # Log invalid rows
    if invalid_dates:
        with open(file_name, "a") as fs:
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
            for idx in invalid_dates:
                fs.write(f'''At {timestamp} -> invalid date in index at {idx} in file {file_name}
------------------------------------------------------------------------------------------------------------

''')
    
    # Keep only valid rows
    return df[is_valid].copy()


def val_check_for_col(df:pd.DataFrame,col:str,file_name:str):
    '''
        this function checks if the value of the column is greater than zero and is an instance of float.
        If not, it sets the value as NaN.
    '''
    def is_valid_value(x):
        try:
            val = float(x)
            return val > 0
        except:
            return False
    is_valid = df[col].apply(is_valid_value)
    invalid_index = df[~is_valid].index.tolist()
    # Log file
    with open(f"hist.log", "a") as fs:
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S")
        for idx in invalid_index:
            fs.write(f'''At {timestamp} time -> invalid value in column "{col}" at code {idx} in file {file_name}
------------------------------------------------------------------------------------------------------------

''')
    # Set invalid values to NaN
    df.loc[~is_valid, col] = np.nan
    return df

def apply_dq_on_hist(df:pd.DataFrame,file_name:str):
    df=col_check(df,file_name)
    for col in df.columns:
        df=val_check_for_col(df,col,file_name)
    df.to_csv('new.csv',index=False)
    return df

    
