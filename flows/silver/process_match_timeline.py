import json

import pandas as pd
from prefect import flow
from prefect.variables import Variable

BRONZE = 'bronze'
SILVER = 'silver'
ENTITY_TIMELINE = 'match/timeline'
ENTITY_STATS = 'match/stats'

TEAM_SIZE = 10

PROCESS_EVENTS = {
    'CHAMPION_KILL',
    'ELITE_MONSTER_KILL',
    'BUILDING_KILL',
}

def process_frames(timeline, max_frame=15):
    parsed_events = []
    for idx, frame in enumerate(1, timeline['info']['frames']):
        if idx >= max_frame:
            break

        events = frame['events']
        for ev in events:
            if ev['type'] not in PROCESS_EVENTS:
                continue

            data = {
                'type': ev.get('type'),
                'principal': ev.get('killerId'),
                'assists': ev.get('assistingParticipantIds', []),
            }

            match ev['type']:
                case 'CHAMPION_KILL':
                    parsed_events.append({
                        'team_id': 100 if (TEAM_SIZE / 2) >= ev['killerId'] else 200,
                        **data,
                        'objective': ev['victimId'],
                    })
                case 'ELITE_MONSTER_KILL':
                    parsed_events.append({
                        'team_id': ev['killerTeamId'],
                        **data,
                        'objective': ev['monsterType'],
                    })
                case 'BUILDING_KILL':
                    parsed_events.append({
                        'team_id': ev['teamId'],
                        **data,
                        'objective': ' '.join([ev['laneType'], ev['buildingType']]),
                    })
                case _:
                    print('Not able to process event type', ev['type'])


    return parsed_events

RELEVANT_COLUMNS = (
    'team_id',
    'totalGold',
    'jungleMinionsKilled',
    'minionsKilled',
    'xp',
    'level',
    'totalDamageDone',
    'totalDamageDoneToChampions',
    'totalDamageTaken',
)

RELEVANT_DAMAGE_FIELDS = (
    'totalDamageDone',
    'totalDamageDoneToChampions',
    'totalDamageTaken',
)

def extract_player_match_data(match, max_frame=15):
    df = pd.DataFrame.from_dict(match['info']['frames'][max_frame]['participantFrames'], orient='index')

    for field in RELEVANT_DAMAGE_FIELDS:
        df[field] = df['damageStats'].apply(lambda x: x.get(field, None))

    df['team_id'] = [100 if (TEAM_SIZE / 2) >= int(idx) else 200 for idx in df.index]

    return df[[*RELEVANT_COLUMNS]]


@flow
async def process_match_timeline(match_id, frame):
    data_path = await Variable.get('data_path')
    data_path = data_path.value
    bronze_path = f'{data_path}/{BRONZE}/{ENTITY_TIMELINE}/{match_id}.json'
    silver_timeline_path = f'{data_path}/{SILVER}/{ENTITY_TIMELINE}/{match_id}.parquet'
    silver_stats_path = f'{data_path}/{SILVER}/{ENTITY_STATS}/{match_id}.parquet'

    with open(bronze_path) as f:
        timeline = json.load(f)

    events = process_frames(timeline, max_frame=15)
    df = pd.DataFrame(events)
    df = df.dropna(axis=0)
    df['objective'] = df['objective'].apply(lambda x: str(x)) # Issue: https://github.com/mage-ai/mage-ai/pull/4857
    
    player_stats = extract_player_match_data(timeline, max_frame=15)
    
    df.to_parquet(silver_timeline_path)
    
    player_stats.to_parquet(silver_stats_path)


def process_match_timeline_local(match_id):
    data_path = '../../'
    bronze_path = f'{data_path}/{BRONZE}/{ENTITY_TIMELINE}/{match_id}.json'

    with open(bronze_path) as f:
        timeline = json.load(f)

    events = process_frames(timeline, max_frame=15)
    player_stats = extract_player_match_data(timeline, max_frame=15)

    df = pd.DataFrame(events)
    df = df.dropna(axis=0)
    return df, player_stats

if __name__ == '__main__':
    match_id = 'EUW1_7040087248'
    events, player_stats = process_match_timeline_local(match_id)
