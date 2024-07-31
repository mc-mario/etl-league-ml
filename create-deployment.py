from prefect.deployments import Deployment
from prefect.filesystems import LocalFileSystem, GitHub
from hello_world_flow import hello_world_flow

from prefect_github.repository import GitHubRepository

github_repository_block = GitHubRepository.load("etl-league-ml")

deployment = Deployment.build_from_flow(
    flow=hello_world_flow,
    name="hello-world-deployment",
    storage=github_repository_block,
    path="hello_world_flow.py",
    work_queue_name="default",
    infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
)

if __name__ == "__main__":
    deployment.apply()