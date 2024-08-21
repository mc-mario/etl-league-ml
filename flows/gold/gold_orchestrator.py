import asyncio
import os

from markdown_it.rules_inline import entity
from prefect import get_run_logger, flow
from prefect.variables import Variable

from flows.utils.db import db_create_session, get_match_id, complete_step

SILVER = 'silver'
GOLD = 'gold'
ENTITY_DETAILS = 'match/details'
ENTITY_TIMELINE = 'match/timeline'
ENTITY_STATS = 'match/stats'

@flow
async def orchestrate_gold_etl(match_id=None):
    logger = get_run_logger()
    session = await db_create_session()
    data_path = await Variable.get('data_path')
    data_path = data_path.value

    if match_id is None:
        match_id = get_match_id(session, {'bronze': True, 'silver': True, 'gold': False})
        complete_step(session, match_id, 'gold', True)

    logger.info(f'Consolidating {match_id}')

    all_data_available = all(
        os.path.isfile(f"{data_path}/{SILVER}/{entity}/{match_id}.parquet")
            for entity in {ENTITY_DETAILS, ENTITY_TIMELINE, ENTITY_STATS}
    )
    if not all_data_available:
        complete_step(session, match_id, 'is_deleted', True)
        logger.info(f'Missing some data points, marking {match_id} as deleted')
        return


if __name__ == '__main__':
    asyncio.run(orchestrate_gold_etl(match_id='EUW1_7005980981'))