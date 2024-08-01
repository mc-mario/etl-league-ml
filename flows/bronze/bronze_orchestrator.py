import json
import os
from datetime import date

from prefect import get_client, flow, task
from prefect.deployments import run_deployment
from prefect.variables import Variable


@flow
async def orchestrate_daily_division_retrieval():
    await list_division_today()
    await fetch_user_data()


@task
async def fetch_user_data():
    get_player_info_deploy = await get_client().read_deployment_by_name(
        name='get-player-information/get_player_information'
    )

    data_path = await Variable.get('data_path')
    division_path = f'{data_path.value}/bronze/division/'

    today_files = filter(
        lambda fi: fi.rstrip('.json').endswith(f'{date.today()}'),
        os.listdir(division_path)
    )

    for file in today_files:
        with open(f'{division_path}/{file}', 'r') as f:
            data = json.load(f)
            for summoner in data:
                run_deployment(
                    get_player_info_deploy.id,
                    {'summoner_id': summoner.get('summonerId')}
                )


@task
async def list_division_today():
    list_division_players_deploy = await get_client().read_deployment_by_name(
        name='list-division-players/list_division_players'
    )
    for division in ['I', 'II', 'III', 'IV']:
        parameters = dict(
            tier='DIAMOND', division=division, queue='RANKED_SOLO_5x5'
        )
        await run_deployment(list_division_players_deploy.id, parameters=parameters)
