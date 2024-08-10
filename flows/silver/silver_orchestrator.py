from prefect import flow, get_client
from prefect.deployments import run_deployment

from flows.utils.db import get_match_id, db_create_session


@flow
async def orchestrate_silver_etl(match_id=None, frame=15):
    session = await db_create_session()

    if match_id is None:
        match_id = get_match_id(session, {'bronze': True, 'silver': False})


    process_match_details = await get_client().read_deployment_by_name(
        name='process-match-details/process_match_details'
    )

    is_processable = await run_deployment(
        process_match_details.id,
        parameters={'match_id': match_id}
    )
    print(is_processable)

    if not is_processable:
        return

    process_match_timeline = await get_client().read_deployment_by_name(
        name='process-match-timeline/process_match_timeline'
    )
    await run_deployment(
        process_match_timeline.id,
        parameters={'match_id': match_id, 'frame': frame}
    )