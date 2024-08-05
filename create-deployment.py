from datetime import timedelta

from prefect.client.schemas.schedules import CronSchedule, IntervalSchedule
from prefect.filesystems import GitHub

from flows.bronze.bronze_orchestrator import orchestrate_daily_division_retrieval, update_bronze_etl_database, \
    get_pending_match
from flows.bronze.get_match_information import get_match_information
from flows.bronze.get_player_information import get_player_information
from flows.bronze.list_division_players import list_division_players
from flows.silver.process_match_timeline import process_match_timeline

github_block = GitHub.load("github")


def deploy_bronze_etl():
    list_division_players.from_source(
        source=github_block,
        entrypoint="flows/bronze/list_division_players.py:list_division_players",
    ).deploy(
        name="list_division_players",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
        tags=['bronze', 'seed'],
    )
    get_player_information.from_source(
        source=github_block,
        entrypoint="flows/bronze/get_player_information.py:get_player_information",
    ).deploy(
        name="get_player_information",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
        tags=['bronze'],
    )
    get_match_information.from_source(
        source=github_block,
        entrypoint="flows/bronze/get_match_information.py:get_match_information",
    ).deploy(
        name="get_match_information",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
        tags=['bronze'],
    )
    orchestrate_daily_division_retrieval.from_source(
        source=github_block,
        entrypoint="flows/bronze/bronze_orchestrator.py:orchestrate_daily_division_retrieval",
    ).deploy(
        name="bronze_orchestrator",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
        tags=['bronze', 'orchestrator'],
        schedule=(CronSchedule(cron="0 3 * * *", timezone="Europe/Madrid")),
    )
    update_bronze_etl_database.from_source(
        source=github_block,
        entrypoint="flows/bronze/bronze_orchestrator.py:update_bronze_etl_database",
    ).deploy(
        name="update_bronze_etl_database",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
        tags=['bronze'],
        schedule=(CronSchedule(cron="0 6 * * *", timezone="Europe/Madrid")),
    )

    get_pending_match.from_source(
        source=github_block,
        entrypoint="flows/bronze/bronze_orchestrator.py:get_pending_match",
    ).deploy(
        name="get_pending_match",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
        tags=['bronze'],
        schedule=IntervalSchedule(interval=timedelta(seconds=10), timezone="Europe/Madrid"),
    )

def deploy_silver_etl():
    process_match_timeline.from_source(
        source=github_block,
        entrypoint="flows/silver/process_match_timeline.py:process_match_timeline",
    ).deploy(
        name="process_match_timeline",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
        tags=['silver'],
    )

if __name__ == "__main__":
    deploy_bronze_etl()
    deploy_silver_etl()

