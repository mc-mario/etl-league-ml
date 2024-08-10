import json
from pprint import pprint

import pandas as pd
from prefect import flow
from prefect.variables import Variable

BRONZE = 'bronze'
SILVER = 'silver'
ENTITY = 'match/details'

COLUMNS_TO_STUDY = {
    'gameDuration',
    'gameMode',
    'gameVersion',
    'gameCreation',
    'participants'
}

PARTICIPANTS_COLUMNS = {
    'championName',
    'lane',
    'role',
}

def process_metadata(details_path):
    with open(details_path) as f:
        details = json.load(f)
        info = details['info']

    for k, v in info.items():
        if k not in COLUMNS_TO_STUDY:
            continue

        if k == 'particpants':
            print(k)
            #for p in v:
            #    pprint(p)


    return info


@flow
def process_match_details(match_id):
    data_path = Variable.get('data_path')
    data_path = data_path.value
    bronze_path = f'{data_path}/{BRONZE}/{ENTITY}/{match_id}.json'
    silver_path = f'{data_path}/{SILVER}/{ENTITY}/{match_id}.parquet'

    metadata = process_metadata(bronze_path)

def process_match_details_local(match_id):
    data_path = '../..'
    bronze_path = f'{data_path}/{BRONZE}/{ENTITY}/{match_id}.json'

    metadata = process_metadata(bronze_path)

    #df = pd.DataFrame(details)
    #df = df.dropna(axis=0)

    return metadata


if __name__ == '__main__':
    match_id = 'EUW1_7023586604'
    df = process_match_details_local(match_id)