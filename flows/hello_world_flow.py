from prefect import flow, task



@task
def say_hello():
    print("Hello, World!")
    with open('/opt/prefect/flows/helloworld.txt', "w") as f:
        f.write("Hello, World!")

@flow
def hello_world_flow():
    say_hello()