import json
import os

import pandas as pd
from prefect import task
from prefect.cli import flow
from prefect.variables import Variable

BRONZE = 'bronze'
SILVER = 'silver'
ENTITY = 'match/timeline'
TEAM_SIZE = 10

@task
def process_frames(timeline_path):
    with open(timeline_path) as f:
        timeline = json.load(f)
        parsed_events = []
        for frame in timeline['info']['frames']:
            events = frame['events']
            for ev in events:
                if ev['type'] not in ('CHAMPION_KILL', 'ELITE_MONSTER_KILL', 'BUILDING_KILL', 'CHAMPION_SPECIAL_KILL'):
                    continue

                match ev['type']:
                    case 'CHAMPION_KILL':
                        data = {'type': ev.get('type'),
                                'assists': ev.get('assistingParticipantIds', []),
                                'killer': ev['killerId'],
                                'victim': ev['victimId'],
                                'team_id': 100 if ev['killerId'] <= (TEAM_SIZE / 2) else 200},
                    case 'ELITE_MONSTER_KILL':
                        data = {'type': ev.get('type'),
                                #'monster': ev.get('monsterType'),
                                'killer': ev['killerId'],
                                'assists': ev.get('assistingParticipantIds', []),
                                'team_id': ev['killerTeamId'],
                                'objective': ev.get('monsterType')}
                    case 'BUILDING_KILL':
                        data = {'type': ev.get('type'),
                                'killer': ev.get('killerId'),
                                'assists': ev.get('assistingParticipantIds', []),
                                'team_id': ev['teamId'],
                                'objective': ' '.join([ev['buildingType'], ev.get('towerType', ''), ev['laneType']]),
                                }
                parsed_events.append(data)

        return parsed_events


@flow
def process_match_timeline(match_id):
    data_path = Variable.get('data_path')

    bronze_path = f'{data_path}/{BRONZE}/{ENTITY}/{match_id}.json'
    silver_path = f'{data_path}/{SILVER}/{ENTITY}/{match_id}.parquet'

    events = process_frames(bronze_path)
    pd.DataFrame(events).to_parquet(silver_path)


if __name__ == '__main__':
    match_id = 'EUW1_6353764274'
    process_match_timeline(match_id)