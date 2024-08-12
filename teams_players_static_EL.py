# %%
import pandas as pd
import time
from sqlalchemy import create_engine,exc,text
from nba_api.stats.static import players,teams
import numpy as np
import psycopg2

# %%
from utility_scripts import config_params,engine_creation

# %%
pd.set_option('display.max_column',None)

# %%
def extract_players():
    player_list = players.get_players()
    players_df = pd.DataFrame(player_list)
    return players_df

def extract_teams():
    team_list = teams.get_teams()
    teams_df = pd.DataFrame(team_list)
    return teams_df


# %%


# %%
def load_tables(df:pd.DataFrame,table_name):
    try:
        db_config = config_params('config.txt')
        print("engine creation started")
        engine = engine_creation(dialect = db_config['dialect'],driver=db_config['driver'],username=db_config['username'],password=db_config['password'],host=db_config['host'],port=db_config['port'],dbname=db_config['database'])
        print("engine creation end: ",engine)   
        df.to_sql(table_name,con = engine,if_exists='replace')
    except Exception as e:
        print("Error occured while loading: ",e)

# %%
nba_players = extract_players()
nba_teams  = extract_teams()
load_tables(nba_players,'Players')
load_tables(nba_teams,'Teams')





# %%
