from airflow.models.dag import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime

with DAG(
    'example_secrets_dag',
    start_date=datetime(2022, 1, 1),
    schedule=None
):

    @task
    def print_var():
        my_var = Variable.get("my_var")
        print(f"My secret variable is: {my_var}")

        conn = BaseHook.get_connection(conn_id="ggogle")
        print(f"My secret connection is: {conn.get_uri()}")

    print_var()
