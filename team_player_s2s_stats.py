# %%
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine,exc,text
from nba_api.stats.endpoints import leaguedashteamstats,leaguedashplayerstats,leaguedashplayerbiostats
import import_ipynb
import configparser


# %%
from utility_scripts import engine_creation,config_params


# %%
def nba_ld_playerbiostats(start_season,end_season)-> pd.DataFrame:
    seasons = [f"20{i}-{i+1}" for i in range(start_season,end_season)]
    season_types = ['Regular Season','Playoffs']
    columns = leaguedashplayerbiostats.LeagueDashPlayerBioStats().get_data_frames()[0].columns.to_list()+['Season','Season_type']
    all_dfs = []
    print(columns)
    for season in seasons:
        for season_type in season_types:
            print(f"{season} - {season_type} is inserting....")
            df = leaguedashplayerbiostats.LeagueDashPlayerBioStats(season = season,season_type_all_star = season_type).get_data_frames()[0]
            df['Season'] = season
            df['Season_Type'] = season_type
            all_dfs.append(df)
            
    print(f"dfs are concatinating....")        
    nba_ld_playerbiostats = pd.concat(all_dfs)
    
    return nba_ld_playerbiostats

# %%
def nba_ld_palyerstats(start_season,end_season)-> pd.DataFrame:
    seasons = [f"20{i}-{i+1}" for i in range(start_season,end_season)]
    season_types = ['Regular Season','Playoffs']
    columns = leaguedashplayerstats.LeagueDashPlayerStats().get_data_frames()[0].columns.to_list()+['Season','Season_type']
    print(columns)
    all_dfs = []
    
    for season in seasons:
        for season_type in season_types:
            print(f"{season} - {season_type} is inserting....")
            df = leaguedashplayerstats.LeagueDashPlayerStats(season=season,season_type_all_star=season_type).get_data_frames()[0]
            df['Season'] = season
            df['Season_Type'] = season_type
            all_dfs.append(df)

    print(f"dfs are concatinating....")
    nba_ld_playerstats = pd.concat(all_dfs)
    return nba_ld_playerstats


# %%
def nba_ld_teamstats(start_season,end_season)->pd.DataFrame:
    seasons = [f"20{i}-{i+1}" for i in range(start_season,end_season)]
    season_types = ['Regular Season','Playoffs']
    columns = leaguedashteamstats.LeagueDashTeamStats().get_data_frames()[0].columns.to_list() + ['Season','Season_Type']
    print(columns)
    all_dfs = []
    
    for s in seasons:
        for st in season_types:
            print(f"{s} - {st} is inserting....")
            df = leaguedashteamstats.LeagueDashTeamStats(season = s,season_type_all_star=st).get_data_frames()[0]
            df['Season'] = s
            df['Season_Type'] = st
            all_dfs.append(df)
            
    print(f"dfs are concatinating....")            
    nba_ld_teamstats = pd.concat(all_dfs)
    return nba_ld_teamstats

# %%
def load_tables(table_name:str,df:pd.DataFrame):
    try:
        db_config = config_params('config.txt')
        print("engine creation started")
        engine = engine_creation(dialect = db_config['dialect'],driver=db_config['driver'],username=db_config['username'],password=db_config['password'],host=db_config['host'],port=db_config['port'],dbname=db_config['database'])
        print("engine creation end: ",engine)   
        print('Creating table: ',table_name)
        df.to_sql(table_name,con = engine,if_exists='append',index=False)
        print(f"{table_name} table have been cerated/appended")
    except Exception as e:
        print(f"Error occured while creating/appending {table_name} table: {e}")



# %%
def transformation_player_stats(start,end):
    print("Extracting: nba_ld_palyerstats_df")
    nba_ld_palyerstats_df = nba_ld_palyerstats(start,end)
    print("Extraction completed")

    print("Transformation player_stats and player_stats_rank started")
    # player stats rank tranformation
    player_stats_rank = nba_ld_palyerstats_df[['PLAYER_ID', 'PLAYER_NAME', 'NICKNAME', 'TEAM_ID', 'TEAM_ABBREVIATION','GP_RANK', 'W_RANK', 'L_RANK', 'W_PCT_RANK', 'MIN_RANK', 'FGM_RANK', 'FGA_RANK', 'FG_PCT_RANK', 'FG3M_RANK', 'FG3A_RANK', 'FG3_PCT_RANK', 'FTM_RANK', 'FTA_RANK', 'FT_PCT_RANK', 'OREB_RANK', 'DREB_RANK', 'REB_RANK', 'AST_RANK', 'TOV_RANK', 'STL_RANK', 'BLK_RANK', 'BLKA_RANK', 'PF_RANK', 'PFD_RANK', 'PTS_RANK', 'PLUS_MINUS_RANK', 'NBA_FANTASY_PTS_RANK', 'DD2_RANK', 'TD3_RANK', 'WNBA_FANTASY_PTS_RANK', 'Season', 'Season_Type']]
    player_stats_rank.drop(columns=['PLAYER_NAME', 'NICKNAME','TEAM_ABBREVIATION','NBA_FANTASY_PTS_RANK', 'DD2_RANK', 'TD3_RANK', 'WNBA_FANTASY_PTS_RANK'],inplace=True)
   
    # player_stats transformation
    player_stats = nba_ld_palyerstats_df[['PLAYER_ID', 'TEAM_ID',
            'GP', 'W', 'L', 'W_PCT', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M',
        'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST',
        'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS','Season', 'Season_Type']]
    
    print("Transformation player_stats and player_stats_rank ended")
    print("Extracting: nba_ld_playerbiostats_df")
    nba_ld_playerbiostats_df = nba_ld_playerbiostats(start,end)
    print("Extraction completed")
    
    print("Transformation nba_ld_playerbiostats_df started")
    # biostats tranformation
    nba_ld_playerbiostats_df = nba_ld_playerbiostats_df[['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'AGE', 'PLAYER_HEIGHT', 'PLAYER_HEIGHT_INCHES', 'PLAYER_WEIGHT', 'COLLEGE', 'COUNTRY', 'DRAFT_YEAR', 'DRAFT_ROUND', 'DRAFT_NUMBER', 'Season', 'Season_Type']]
    nba_ld_playerbiostats_df['Nickname']=pd.merge(left=nba_ld_playerbiostats_df,right=nba_ld_palyerstats_df,how='inner',on=['PLAYER_ID','Season','Season_Type'])['NICKNAME']
    print("Transformation nba_ld_playerbiostats_df ended")
    
    return nba_ld_playerbiostats_df,player_stats,player_stats_rank

    

def transformation_team_stats(start,end):
    print("Extracting: nba_ld_teamstats_df")
    nba_ld_teamstats_df = nba_ld_teamstats(start,end)
    print("Extraction completed")

    print("Transformation Teams_Stats started")    
    #  Teams_Stats transformation
    teams_stats = nba_ld_teamstats_df[['TEAM_ID', 'GP', 'W', 'L', 'W_PCT', 'MIN', 'FGM', 'FGA',
        'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB',
        'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PTS',
        'PLUS_MINUS','Season', 'Season_Type']]


    # team_stats_ranks transformation
    team_stats_ranks = nba_ld_teamstats_df[['TEAM_ID','GP_RANK', 'W_RANK', 'L_RANK', 'W_PCT_RANK', 'MIN_RANK',
        'FGM_RANK', 'FGA_RANK', 'FG_PCT_RANK', 'FG3M_RANK', 'FG3A_RANK',
        'FG3_PCT_RANK', 'FTM_RANK', 'FTA_RANK', 'FT_PCT_RANK', 'OREB_RANK',
        'DREB_RANK', 'REB_RANK', 'AST_RANK', 'TOV_RANK', 'STL_RANK', 'BLK_RANK',
        'BLKA_RANK', 'PF_RANK', 'PFD_RANK', 'PTS_RANK', 'PLUS_MINUS_RANK',
        'Season', 'Season_Type']]
    print("Transformation Teams_Stats ended")
    
    return teams_stats,team_stats_ranks


# %%

#nba_ld_teamstats_df = nba_ld_teamstats(12,24)


teams_stats,team_stats_ranks = transformation_team_stats(12,24)
nba_ld_playerbiostats_df,player_stats,player_stats_rank = transformation_player_stats(12,24)

# %%
load_tables(table_name = 'Teams_Stats',df=teams_stats)
load_tables(table_name = 'Teams_Stats_Ranks',df=team_stats_ranks)

load_tables(table_name = 'Players_Stats',df=player_stats)
load_tables(table_name = 'Players_BioStats',df=nba_ld_playerbiostats_df)
load_tables(table_name = 'Players_Stats_Ranks',df=player_stats_rank)




# %%



