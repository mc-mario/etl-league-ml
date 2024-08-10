import json

from prefect import task, flow
from prefect.variables import Variable
from pulsefire.clients import RiotAPIClient


@task
async def get_match_timeline(match_id, client_kwargs):
    async with RiotAPIClient(**client_kwargs) as client:
        return await client.get_lol_match_v5_match_timeline(id=match_id, region='europe')


@task
async def get_match(match_id, client_kwargs):
    async with RiotAPIClient(**client_kwargs) as client:
        return await client.get_lol_match_v5_match(id=match_id, region='europe')


@flow
async def get_match_information(match_id):
    API_KEY = await Variable.get('riot_api_key')

    client_kwargs = dict(
        default_headers={"X-Riot-Token": API_KEY.value}
    )
    timeline = await get_match_timeline(match_id, client_kwargs)
    details = await get_match(match_id, client_kwargs)

    data_path = await Variable.get('data_path')
    path = data_path.value
    with open(f'{path}/bronze/match/timeline/{match_id}.json', 'w+') as f:
        json.dump(timeline, f, indent=4)

    with open(f'{path}/bronze/match/details/{match_id}.json', 'w+') as f:
        json.dump(details, f, indent=4)

