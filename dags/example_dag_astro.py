import json
from datetime import datetime, timedelta
from airflow import DAG
from astro.sql import append, dataframe, load_file, run_raw_sql, transform
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
import pandas as pd

@task(task_id='transform')
def transform(xcom: str) -> str:
    a = pd.read_json(xcom, orient='index')
    return a

@task(task_id='extract')
def extract() -> str:
    """
    #### Extract task
    A simple "extract" task to get data ready for the rest of the
    pipeline. In this case, getting data is simulated by reading from a
    hardcoded JSON string.
    """
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

    order_data_dict = json.loads(data_string)
    return json.dumps(order_data_dict)
    
@dataframe
def load(input: pd.DataFrame) -> None:
    """
    #### Load task
    A simple "load" task that takes in the result of the "transform" task and prints it out,
    instead of saving it to end user review
    """

    print(input)

with DAG(
    "example_dag_astro",
    schedule_interval="@daily",
    start_date=datetime(2022, 11, 11),
    catchup=False,
) as dag:
    
    order_data = extract()
    transform_data = transform(order_data)
    #order_summary = transform(order_data)
    load(transform_data)
    #extracted_joined_df_table = extract_joined_table()
