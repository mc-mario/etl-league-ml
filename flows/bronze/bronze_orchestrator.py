from prefect import get_client, flow
from prefect.deployments import run_deployment


@flow
async def bronze_orchestrator():
    deployment = await get_client().read_deployment_by_name(
        name='get_match_information/get_match_information'
    )

    match_id = 'EUW1_6994259938'
    await run_deployment(deployment.id, parameters={'match_id': match_id})

