from prefect.filesystems import GitHub

from flows.bronze.list_division_players import list_division_players
from flows.hello_world_flow import hello_world_flow

github_block = GitHub.load("github")

if __name__ == "__main__":
    # hello_world_flow.from_source(
    #     source=github_block,
    #     entrypoint="flows/hello_world_flow.py:hello_world_flow",
    # ).deploy(
    #     name="hello2",
    #     work_pool_name="default-agent-pool",
    #     work_queue_name="default",
    # )

    list_division_players.from_source(
        source=github_block,
        entrypoint="flows/bronze/list_division_players.py:list_division_players",
    ).deploy(
        name="list_division_players",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
    )