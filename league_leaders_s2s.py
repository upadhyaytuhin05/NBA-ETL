# %%
import pandas as pd
import numpy as np
import time
import requests
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine,exc
from utility_scripts import Load_Table

# %%
pd.set_option('display.max_columns',None)

# %%
def league_leader_dataset(year,season_type):
    data_url = f"https://stats.nba.com/stats/leagueLeaders?LeagueID=00&PerMode=Totals&Scope=S&Season={year}&SeasonType={season_type}&StatCategory=PTS"
    #print(data_url)
    r = requests.get(url = data_url).json()
    tbl_headers = r['resultSet']['headers']
    league_leader_df = pd.DataFrame(r['resultSet']['rowSet'],columns=tbl_headers)
    #print(league_leader_df.head())
    league_leader_df['SEASON'] = year
    league_leader_df['SEASON_TYP'] = season_type
    return league_leader_df

# %%
def insert_dataset(df):
    seasons = [f'20{i}-{i+1}' for i in range(12,24)]
    season_typ = ['Regular%20Season','Playoffs']
    
    for s in seasons:
        for st in season_typ:
            append_data = league_leader_dataset(s,st)
            df = pd.concat([df,append_data],ignore_index=True)
    
    return df
        

# %%
league_leader_test,tbl_headers = league_leader_dataset('2021-22','Playoffs')

# %%

# %% [markdown]
# ### Initialising dataset

# %%
league_leader_df = pd.DataFrame(columns = tbl_headers)

league_leader_df.head()


# %% [markdown]
# ### Inserting data into dataset
# 

# %%
league_leader_df = insert_dataset(league_leader_df)

# %%
league_leader_df.shape

# %%
league_leader_df.head()

# %% [markdown]
# ### Basic Transforms

# %%
league_leader_df.columns

# %%
league_leader_df.rename(columns={'GP':'Games_Played',
                                 'FGA':'Field_Goals_Attempted',
                                 'FGM':'Field_Goals_Made',
                                 'FG_PCT':'Field_Goal_Pct',
                                 'FG3M':'3_Pt_Atmpt',
                                 'FG3_PCT':'3_Pt_Pct',
                                 'MIN':'Minutes_Played',
                                 'FTM':'Free_Throws_Made',
                                 'FTA':'Free_Throws_Attempted',
                                 'FT_PCT':'Free_Throws_Pct',
                                 'OREB':'Off_Rebounds',
                                 'DREB':'Deff_Rebounds',
                                 'REB':'Rebounds',
                                 'AST':'Assists',
                                 'STL':'Steals',
                                 'BLK':'Blocks',
                                 'TOV':'Trn_Ovrs',
                                 'PF': 'Personal_Fouls',
                                 'PTS':'Points',
                                 'AST_TOV':'Ast_Trn_Ovrs',
                                 'STL_TOV':'Stl_Trn_Ovrs'
                                },inplace=True)

# %%
def engine_crt(dialect,driver,username,password,dbname,host,port):
    engine_url = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{dbname}"
    print(engine_url)
    engine = create_engine(engine_url,isolation_level = 'AUTOCOMMIT')
    return engine

# %%
Load_Table('League_Leaders_s2s',league_leader_df,'append')

# %%
conn = engine.connect()
conn.execute(sqlalchemy.text("select * from nba_dataset.League_Leaders"))

# %%
nba_test = pd.DataFrame()

# %%
nba_concat = pd.DataFrame(columns = ['x','y'])

# %%
nba_test = nba_test.concat(nba_concat)

# %%


# %%



