from prefect import Flow, Parameter, task
import seaborn as sns
import pandas as pd
from sklearn.preprocessing import scale

@task
def load_data() -> pd.DataFrame:
    return sns.load_dataset("tips")

@task
def encode_categories(data: pd.DataFrame) -> pd.DataFrame:
    return pd.get_dummies(data)

@task
def standard_scale(data: pd.DataFrame) -> pd.DataFrame:
    cols_to_scale = ["total_bill", "tip"]
    data[cols_to_scale] = data[cols_to_scale].apply(lambda x: (x - x.mean()) / x.std())
    return data

@task
def persist(data: pd.DataFrame) -> None:
    data.to_csv("tips.csv")

with Flow("preprocessing") as flow:
    data = load_data()
    encoded_data = encode_categories(data)
    scaled_data = standard_scale(encoded_data)
    persist(scaled_data)
    
flow.run()