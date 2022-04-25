#%%
from prefect import task, Flow, Parameter
from get_prices import get_prices
from database_helpers import write_to_db
from prefect.tasks.shell import ShellTask

#%%
st = ShellTask("echo 'Hello World'")
with Flow("demo-etl") as flow:
    ticker = Parameter("ticker")

    data = get_prices(ticker)
    content = st
    print(content)
    write_to_db(data, dump=content)

state = flow.run({"ticker": "AAPL"})

#%%
# flow.visualize(state)