from prefect import get_client, flow
from prefect.deployments import run_deployment


@flow
async def orchestrate_daily_division_retrieval():
    deployment = await get_client().read_deployment_by_name(
        name='list-division-players/list_division_players'
    )

    for divisions in ['I', 'II', 'III', 'IV']:
        parameters = dict(
            tier='DIAMOND', division='I', queue='RANKED_SOLO_5x5'
        )
        await run_deployment(deployment.id, parameters=parameters)

