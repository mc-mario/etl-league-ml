from prefect.filesystems import GitHub

from flows.bronze.get_match_information import get_match_information
from flows.bronze.get_player_information import get_player_information
from flows.bronze.list_division_players import list_division_players


github_block = GitHub.load("github")

if __name__ == "__main__":
    list_division_players.from_source(
        source=github_block,
        entrypoint="flows/bronze/list_division_players.py:list_division_players",
    ).deploy(
        name="list_division_players",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
    )

    get_player_information.from_source(
        source=github_block,
        entrypoint="flows/bronze/get_player_information.py:get_player_information",
    ).deploy(
        name="get_player_information",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
    )

    get_match_information.from_source(
        source=github_block,
        entrypoint="flows/bronze/get_match_information.py:get_match_information",
    ).deploy(
        name="get_match_information",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
    )

