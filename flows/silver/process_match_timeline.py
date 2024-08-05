import json
import os

import pandas as pd
from prefect import task
from prefect.cli import flow
from prefect.variables import Variable

BRONZE = 'bronze'
SILVER = 'silver'
ENTITY = 'match/timeline'

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
                        data = {'assists': ev.get('assistingParticipantIds', []), 'killer': ev['killerId']}
                    case 'ELITE_MONSTER_KILL':
                        data = {'monster': ev.get('monsterType'), 'team_id': ev['killerTeamId']}
                    case 'BUILDING_KILL':
                        data = {'building': ' '.join([ev['buildingType'], ev.get('towerType', '')]), 'lane': ev['laneType'],
                                'team_id': ev['teamId']}
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
    match_id = 'EUW_0123'
    process_match_timeline(match_id)