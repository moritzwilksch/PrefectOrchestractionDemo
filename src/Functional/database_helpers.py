from prefect import task, Flow
import time

@task
def write_to_db(data: list[tuple], dump):
    time.sleep(1)    
    print("Written to DB")
    print(dump)