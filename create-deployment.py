from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from prefect.infrastructure import Process

from hello_world_flow import hello_world_flow

# Create a deployment for the flow
github_block = GitHub.load("github")  # Replace with your GitHub block name or ID
deployment = Deployment.build_from_flow(
    flow=hello_world_flow,
    name="hello-world-deployment",
    infrastructure=Process(),
    storage=github_block
)

if __name__ == "__main__":
    deployment.apply()