import pandas as pd
import numpy as np
from sqlalchemy import exc,create_engine,text
import time
import configparser

## Config dictionary of DB
def config_params(file_name:str):
    config = configparser.ConfigParser()
    config.read(file_name)
    db_config = config['database']
    return {
        'dialect': db_config['dialect'],
        'driver': db_config['driver'],
        'username': db_config['username'],
        'password': db_config['password'],
        'host': db_config['host'],
        'port': db_config['port'],
        'database': db_config['database']
    }


## Creating engine connection to SQLalchemy

def engine_creation(dialect,driver,username,password,host,port,dbname):
    try:

        print(f"Creating SQLAlchemy engine on DB: {dbname}....")
        engine = create_engine(f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{dbname}")
        print(f"Created SQLAlchemy engine on DB: {dbname}....")
        return engine
    except exc.ProgrammingError() as e:
        print(f"Error occurred while creating engine: {e}")

## Loading Table to postgres
def Load_Table(table_name:str,df:pd.DataFrame,ifexists ):
    try:
        db_config = config_params('config.txt')
        print("engine creation started")
        engine = engine_creation(dialect = db_config['dialect'],driver=db_config['driver'],username=db_config['username'],password=db_config['password'],host=db_config['host'],port=db_config['port'],dbname=db_config['database'])
        print("engine creation end: ",engine)   
        print('Creating table: ',table_name)
        print(f" dataframe is Loading to postgres....")
        df.to_sql(table_name,con=engine,if_exists=ifexists,index=False)
        print(f" dataframe is loaded to postgres")
    except Exception as e:
        print (f"Load Error: {e}")