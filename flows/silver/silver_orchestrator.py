from prefect import flow, get_client, get_run_logger
from prefect.client.schemas import FlowRun, StateType
from prefect.deployments import run_deployment
from prefect.states import Completed

from flows.silver.process_match_details import process_match_details
from flows.silver.process_match_timeline import process_match_timeline
from flows.utils.db import get_match_id, db_create_session, complete_step


@flow
async def orchestrate_silver_etl(match_id=None, frame=15):
    logger = get_run_logger()
    session = await db_create_session()

    if match_id is None:
        match_id = get_match_id(session, {'bronze': True, 'silver': False})
        complete_step(session, match_id, 'silver', True)

    logger.info(f'Grabbed {match_id}')

    success = await process_match_details(match_id=match_id)

    logger.info(f'Running process-match-details')
    #run: FlowRun = await run_deployment(
    #    process_match_details.id,
    #    parameters={'match_id': match_id}
    #)

    logger.info(f'Result of is_processable={success}')

    if not success:
        logger.info(f'{match_id} is not processable, marking as deleted')
        complete_step(session, match_id, 'is_deleted', True)
        return

    logger.info("Processing timeline")
    #process_match_timeline = await get_client().read_deployment_by_name(
    #    name='process-match-timeline/process_match_timeline'
    #)
    #await run_deployment(
    #    process_match_timeline.id,
    #    parameters={'match_id': match_id, 'frame': frame}
    #)
    success = await process_match_timeline(match_id=match_id, frame=frame)
    logger.info(f'Marking {match_id} silver step')