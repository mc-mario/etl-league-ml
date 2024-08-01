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
    data_path = await Variable.get('data_path')
    path = data_path.value

    division_files = os.listdir(f'{path}/bronze/division/')
    today_files = filter(lambda f: f.rstrip('.json').endswith(f'{date.today()}'), division_files)

    get_player_info_deploy = await get_client().read_deployment_by_name(
        name='get-player-information/get_player_information'
    )
    for file in today_files:
        with open(file, 'r') as f:
            data = json.load(f)
            run_deployment(
                get_player_info_deploy.id,
                {'summoner_id': data.get('summonerId')}
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
