import json
from pprint import pprint

import pandas as pd
from prefect import flow
from prefect.variables import Variable

BRONZE = 'bronze'
SILVER = 'silver'
ENTITY = 'match/details'

METADATA_COLUMNS = {
    'gameDuration',
    'gameMode',
    'gameVersion',
    'gameCreation',
    'gameType'
}

PARTICIPANTS_COLUMNS = {
    'championName',
    'teamPosition',
}

def process_metadata(details_path):
    with open(details_path) as f:
        details = json.load(f)
        info = details['info']

    data = {k:v for k, v in info.items() if k in METADATA_COLUMNS}

    for idx, participant in enumerate(info['participants'], 1):
        data[idx] = [participant[col] for col in PARTICIPANTS_COLUMNS] + [details['metadata']['participants'][idx-1]]

    data[f"{info['teams'][0]['teamId']}_winner"] = info['teams'][0]['win']

    return data


@flow
async def process_match_details(match_id):
    data_path = await Variable.get('data_path')
    data_path = data_path.value
    bronze_path = f'{data_path}/{BRONZE}/{ENTITY}/{match_id}.json'
    silver_path = f'{data_path}/{SILVER}/{ENTITY}/{match_id}.json'

    metadata = process_metadata(bronze_path)

    if metadata.get('gameMode') != 'CLASSIC' or metadata.get('gameType') != 'MATCHED_GAME':
        return False

    with open(silver_path) as f:
        json.dump(metadata, f)

    return True


def process_match_details_local(match_id):
    data_path = '../..'
    bronze_path = f'{data_path}/{BRONZE}/{ENTITY}/{match_id}.json'

    metadata = process_metadata(bronze_path)

    #df = pd.DataFrame(details)
    #df = df.dropna(axis=0)

    return metadata


if __name__ == '__main__':
    match_id = 'EUW1_7040087248'
    df = process_match_details_local(match_id)