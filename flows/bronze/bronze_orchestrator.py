import json
import os
from datetime import date, datetime

from prefect import get_client, flow, task
from prefect.deployments import run_deployment
from prefect.variables import Variable

from tinydb import TinyDB, Query

@flow
async def orchestrate_daily_division_retrieval():
    await list_division_today()
    await fetch_user_data()
    await update_bronze_etl_database()

@flow
async def update_bronze_etl_database():
    data_path = await Variable.get('data_path')
    db_path = f"{data_path.value}/etl_status.json"

    db = TinyDB(db_path)
    match_table = db.table('match_ids')
    Match = Query()

    match_files = filter(
        lambda fi: fi.rstrip('.json').endswith(f'matches_{date.today()}'),
        os.listdir(f"{data_path.value}/bronze/players/")
    )

    for match_file in match_files:
        with open(f'{data_path.value}/bronze/players/{match_file}', 'r') as f:
            data = json.load(f)
            for match_id in data:
                if match_table.contains(Match.match_id == match_id):
                    continue

                match_table.insert({
                    'match_id': data['match_id'],
                    'insert_date': datetime.now(),
                    'bronze': False,
                    'silver': False,
                    'gold': False,
                })




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
                await run_deployment(
                    get_player_info_deploy.id,
                    parameters={'summoner_id': summoner.get('summonerId')},
                    flow_run_name=f'run_{date.today()}_{summoner.get("summonerId")}',
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
