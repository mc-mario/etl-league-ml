import json
import os
from datetime import datetime

from prefect import task, flow
from pulsefire.clients import RiotAPIClient

API_KEY = os.getenv('RIOT_API_KEY')

@task
async def get_division(tier='DIAMOND', division='I', queue='RANKED_SOLO_5x5'):
    async with RiotAPIClient(default_headers={"X-Riot-Token": API_KEY}) as client:
        return await client.get_lol_league_v4_entries_by_division(tier=tier, division=division, queue=queue, region='euw1')


@flow
async def list_division_players():
    tier = 'DIAMOND'
    division = 'I'
    queue = 'RANKED_SOLO_5x5'
    resp = await get_division(tier=tier, division=division, queue=queue)
    with open(f'/opt/prefect/flows/bronze/division/{tier}_{division}_{queue}_{datetime.now().timestamp()}') as f:
        json.dumps(resp, f)
