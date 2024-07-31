import json
from datetime import date

from prefect import task, flow
from prefect.variables import Variable
from pulsefire.clients import RiotAPIClient

@task
async def get_player_details(summoner_id, client_kwargs):
    async with RiotAPIClient(**client_kwargs) as client:
        return await client.get_lol_summoner_v4_by_id(id=summoner_id, region='euw1')


@task
async def get_match_history(summoner_puuid, client_kwargs):
    async with RiotAPIClient(**client_kwargs) as client:
        return await client.get_lol_match_v5_match_ids_by_puuid(puuid=summoner_puuid, region='europe')


@flow
async def get_player_information(summoner_id: str):
    API_KEY = await Variable.get('riot_api_key')

    client_kwargs = dict(
        default_headers={"X-Riot-Token": API_KEY.value}
    )
    player_info = await get_player_details(summoner_id, client_kwargs)
    match_history = await get_match_history(player_info["puuid"], client_kwargs)

    path = await Variable.get('data_path').value
    with open(f'{path}/bronze/player/{summoner_id}.json', 'w') as f:
        json.dump(player_info, f, indent=4)

    with open(f'{path}/bronze/player/{summoner_id}_matches_{date.today()}.json', 'w+') as f:
        json.dump(match_history, f, indent=4)

