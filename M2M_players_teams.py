# %%
import pandas as pd
import numpy as np
import time
from datetime import datetime
from sqlalchemy import create_engine,exc,text
from nba_api.stats.endpoints import leaguegamelog

# %%
from utility_scripts import engine_creation,config_params,Load_Table

# %% [markdown]
# ### Player/Team match to match data definition

# %%
## Creating a function to define the dataframe for seasons between year (season_start,season_end), team_player_abv:P or T
def m2m_dataframe_creation(team_player_abv:str,season_start,season_end)->pd.DataFrame:
    seasons = [f"20{i}-{i+1}" for i in range(season_start,season_end)]
    season_types = ["Regular Season","Playoffs"]
    print(f"M2M {team_player_abv} data Dataframe creation begin for seasons between 20{season_start}-20{season_end} ...")
    all_dfs = []
    for s in seasons:
        for st in season_types:
            print(f"    M2M {team_player_abv} data Dataframe creation begin for season={s} and seaon_type={st} .... ")
            df = leaguegamelog.LeagueGameLog(season_type_all_star=st,season=s,player_or_team_abbreviation=team_player_abv).get_data_frames()[0]
            df['Season'] = s
            df['Season_Type'] = st
            all_dfs.append(df)

    nba_m2m_df = pd.concat(all_dfs)
    return nba_m2m_df
                                              
    

# %% [markdown]
# ### Transformation logic

# %%
def transform(df:pd.DataFrame):
    try:
        print("Transformation start ")
        df['Home_Away'] = np.where(df['MATCHUP'].str.contains('vs.',case=False),'H','A')
        df['Created_Time'] = datetime.now()
        df.drop(columns=['VIDEO_AVAILABLE','TEAM_NAME','TEAM_ABBREVIATION'],inplace=True)
        df['Id'] = df['GAME_ID']+'_'+df['Home_Away']
        print("Transformation end ")

        return df
    except Exception as e:
        print("Error occured while transforming in m2m data pipeline: ",df)
    

# %% [markdown]
# ### Engine creation

# %%
engine = engine_creation('postgresql',
 'psycopg2',
 'postgres',
 'root',
 'localhost',
 '5432',
 'nba_dataset')

# %%
type(engine)

# %% [markdown]
# ### Dataframe creation and transformation to be loaded

# %%
nba_m2m_players = m2m_dataframe_creation(team_player_abv='P',season_start=12,season_end=24)


# %%
#Transformation
nba_m2m_teams = m2m_dataframe_creation(team_player_abv='T',season_start=12,season_end=24)


# %%
nba_m2m_players=transform(nba_m2m_players)

# %%
nba_m2m_players.head()

# %%
## Transformation
nba_m2m_teams = transform(nba_m2m_teams)


# %% [markdown]
# ### Loading data to tables in PSQL

# %%
Load_Table(table_name='M2M_Players',df=nba_m2m_players,ifexists='append')
Load_Table(table_name='M2M_Teams',df=nba_m2m_teams,ifexists='append')

# %%
type(engine)

# %%
with engine.connect() as conn:
    result = conn.execute(text("""SELECT * from public."M2M_Players" """))
    result.fetchall()

# %%
result.fetchall()

# %%



