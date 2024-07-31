from prefect.deployments import Deployment
from prefect_github import GitHubCredentials
from prefect_github.repository import GitHubRepository

from hello_world_flow import hello_world_flow

github_repository_block = GitHubRepository.load("etl-league-ml")

# Create a deployment for the hello_world_flow
deployment = Deployment.build_from_flow(
    flow=hello_world_flow,
    name="hello-world-deployment",
    parameters={"filename": "hello_world.txt"},
    storage=github_repository_block,
)

if __name__ == "__main__":
    deployment.apply()