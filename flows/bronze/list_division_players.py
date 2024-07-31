import json
from datetime import date

from prefect import task, flow
from prefect.variables import Variable
from pulsefire.clients import RiotAPIClient


@task
async def get_division(tier, division, queue):
    API_KEY = await Variable.get('riot_api_key')
    async with RiotAPIClient(default_headers={"X-Riot-Token": API_KEY.value}) as client:
        return await client.get_lol_league_v4_entries_by_division(tier=tier, division=division, queue=queue, region='euw1')


@flow
async def list_division_players(tier = 'DIAMOND', division = 'I', queue = 'RANKED_SOLO_5x5'):
    resp = await get_division(tier=tier, division=division, queue=queue)

    path = await Variable.get('data_path')
    with open(f'{path}/bronze/division/{tier}_{division}_{date.today()}.json', 'w') as f:
        json.dump(resp, f, indent=4)
