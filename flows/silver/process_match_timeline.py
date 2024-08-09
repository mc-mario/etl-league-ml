import json

import pandas as pd
from prefect import flow
from prefect.variables import Variable

BRONZE = 'bronze'
SILVER = 'silver'
ENTITY = 'match/timeline'

TEAM_SIZE = 10

PROCESS_EVENTS = {
    'CHAMPION_KILL',
    'ELITE_MONSTER_KILL',
    'BUILDING_KILL',
}
# @task
def process_frames(timeline_path):
    with open(timeline_path) as f:
        timeline = json.load(f)
        parsed_events = []
        for frame in timeline['info']['frames']:
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


@flow
def process_match_timeline(match_id):
    data_path = Variable.get('data_path')
    data_path = data_path.value
    bronze_path = f'{data_path}/{BRONZE}/{ENTITY}/{match_id}.json'
    silver_path = f'{data_path}/{SILVER}/{ENTITY}/{match_id}.parquet'

    events = process_frames(bronze_path)

    df = pd.DataFrame(events)
    df = df.dropna(axis=0)
    df.to_parquet(silver_path)


def process_match_timeline_local(match_id):
    data_path = '../../'
    bronze_path = f'{data_path}/{BRONZE}/{ENTITY}/{match_id}.json'

    events = process_frames(bronze_path)

    df = pd.DataFrame(events)
    df = df.dropna(axis=0)
    return df

if __name__ == '__main__':
    match_id = 'EUW1_6353764274'
    df = process_match_timeline_local(match_id)
