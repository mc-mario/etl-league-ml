import os

import pandas as pd
from hyperframe.frame import FRAMES
from prefect import task
from prefect.variables import Variable

ENTITY = 'match'
ENTITY_DETAILS = f'{ENTITY}/details'
ENTITY_TIMELINE = f'{ENTITY}/timeline'
ENTITY_STATS = f'{ENTITY}/stats'
SILVER = 'silver'
GOLD = 'gold'
FRAMES = '15'

@task
async def aggregate_match_data(data_path='.', match_id='EUW1_7007516257'):
    data_path = await Variable.get('data_path')
    data_path = data_path.value

    df_details  = pd.read_parquet(f"{data_path}/{SILVER}/{ENTITY_DETAILS}/{match_id}.parquet")
    df_timeline = pd.read_parquet(f"{data_path}/{SILVER}/{ENTITY_TIMELINE}/{match_id}.parquet")
    df_stats    = pd.read_parquet(f"{data_path}/{SILVER}/{ENTITY_STATS}/{match_id}.parquet")

    roles_by_id = {int(k): v.lower() for k,v in df_details.iloc[0, 5:-1].to_dict().items()}

    # Metadata
    final_df = pd.DataFrame([{'match_id': match_id}])
    final_df['match_id'] =      str(match_id)
    final_df['gameCreation'] =  df_details['gameCreation']
    final_df['gameDuration'] =  df_details['gameDuration']
    final_df['gameMode'] =      df_details['gameMode']
    final_df['gameType'] =      df_details['gameType']
    final_df['gameVersion'] =   df_details['gameVersion']

    # Target
    final_df['red_winner'] =    df_details['100_winner']
    final_df['blue_winner'] =   ~final_df['red_winner']

    # Neutral Objectives
    for team, team_id in (('red', 100), ('blue', 200)):
        final_df[f'{team}_dragons'] = df_timeline.query(f'objective == "DRAGON" and team_id == {team_id}')['objective'].count()
        final_df[f'{team}_grubs'] = df_timeline.query(f'objective == "HORDE" and team_id == {team_id}')['objective'].count()
        final_df[f'{team}_towers_top_taken'] = df_timeline.query(f'type == "BUILDING_KILL" and objective == "TOP_LANE TOWER_BUILDING" and team_id == {team_id}')['objective'].count()
        final_df[f'{team}_towers_middle_taken'] = df_timeline.query(f'type == "BUILDING_KILL" and objective == "MID_LANE TOWER_BUILDING" and team_id == {team_id}')['objective'].count()
        final_df[f'{team}_towers_bottom_taken'] = df_timeline.query(f'type == "BUILDING_KILL" and objective == "BOT_LANE TOWER_BUILDING" and team_id == {team_id}')['objective'].count()


    # Add player stats to the final dataframe
    for player_id, role in roles_by_id.items():
        team_id = 100 if player_id < 6 else 200
        prefix = 'red' if team_id == 100 else 'blue'
        stats = df_stats.iloc[player_id - 1]

        final_df[f'{prefix}_{role}_damageDoneChampions'] = stats['totalDamageDoneToChampions']
        final_df[f'{prefix}_{role}_totalDamageDone'] = stats['totalDamageDone']
        final_df[f'{prefix}_{role}_damageTaken'] = stats['totalDamageTaken']
        final_df[f'{prefix}_{role}_gold'] = stats['totalGold']
        final_df[f'{prefix}_{role}_level'] = stats['level']
        final_df[f'{prefix}_{role}_minionsKilled'] = stats['minionsKilled'] + stats['jungleMinionsKilled']
        final_df[f'{prefix}_{role}_xp'] = stats['xp']
        final_df[f'{prefix}_{role}_kills'] = df_timeline.query(f'type == "CHAMPION_KILL" and principal == {player_id}')['principal'].count()
        final_df[f'{prefix}_{role}_deaths'] = df_timeline.query(f'type == "CHAMPION_KILL" and objective == "{player_id}"')['objective'].count()
        final_df[f'{prefix}_{role}_assists'] = df_timeline.query('type == "CHAMPION_KILL"')['assists'].apply(lambda assists: player_id in assists).sum()

    final_df.to_parquet(f'{data_path}/{GOLD}/{ENTITY}/{FRAMES}/{match_id}.parquet')

    return final_df, (df_timeline, df_details, df_stats)

if __name__ == '__main__':
    timelines = os.listdir('./silver/match/timeline')
    # for timeline in timelines:
    #     match_id = timeline.split('.parquet')[0]
    #     print(match_id)
    #     await aggregate_match_data(data_path='.', match_id=match_id)
