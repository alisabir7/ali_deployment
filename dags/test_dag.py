from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from contextlib import closing
from pyhive import trino

# Define a function to get Trino connection
def get_trino_connection(user):
    host = 'stx-trino'
    port = 8080

    conn = trino.connect(
        host=host,
        port=int(port),
        protocol='http',
        catalog='mysql',
        username=user,
    )
    return conn

# Default arguments for the DAG
default_args = {
    'owner': 'Ali',
    'depends_on_past': False,
    'retries': 0
}

# Define the DAG
dag = DAG(
    "load_data",
    description="Load data from MySQL table using Trino",
    catchup=False,
    default_args=default_args,
    start_date=datetime(2019, 12, 30),
    tags=["utility"],
    schedule_interval=None
)

# Function to load data from MySQL table using Trino
def load_data_table():
    con = get_trino_connection('933598')
    query = "SELECT * FROM mysql.tmp.test"
    with closing(con.cursor()) as cur:
        cur.execute(query)
        res = cur.fetchall()
    return res    

# Task to load data
def load_data(**kwargs):
    data = load_data_table()
    # Do something with the data here

# Define tasks
task_dummy = DummyOperator(task_id="dummy", dag=dag)

load_data_task = PythonOperator(
    task_id='load_data_task',
    dag=dag,
    python_callable=load_data,
    retries=0
)

# Define task dependencies
task_dummy >> load_data_task
