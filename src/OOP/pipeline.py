#%%
from os import write
from task_definitions import FetchYFTask, WriteToDatabaseTask
from prefect import Flow, task

fetch_task = FetchYFTask()
write_task = WriteToDatabaseTask()

with Flow("etl-pipeline") as flow:
    data = fetch_task(ticker="AAPL")
    write_to_db = write_task(data)

flow.run()

#%%
flow.visualize()