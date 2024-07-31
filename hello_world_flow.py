from prefect import flow, task
import os

@task
def write_hello_world(filename):
    with open(filename, "w") as f:
        f.write("Hello, World!")
    return filename

@flow
def hello_world_flow(filename="hello_world.txt"):
    result_path = write_hello_world(filename)
    print(f"File written to: {result_path}")

if __name__ == "__main__":
    hello_world_flow()