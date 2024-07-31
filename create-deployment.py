from prefect.deployments import Deployment
from prefect.filesystems import GitHub

from flows.hello_world_flow import hello_world_flow

# Create a deployment for the flow
github_block = GitHub.load("github")
deployment = Deployment.build_from_flow(
    flow=hello_world_flow,
    name="hello2",
    storage=github_block,
    entrypoint="flows/hello_world_flow.py:hello_world_flow"
)

if __name__ == "__main__":
    deployment.apply()