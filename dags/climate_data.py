import sys
import os

# adiciona o diretório raiz ao PYTHONPATH
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)


from operations.extract_data import extract_data

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pendulum

import os
from dotenv import load_dotenv

load_dotenv()
file_path = os.getenv("FILE_PATH")

start_date = pendulum.datetime(2025, 10, 27, tz="UTC")

with DAG(
    dag_id="climate_data",
    start_date=start_date,
    # schedule_interval='0 0 * * 1' 
    schedule_interval='*/10 * * * *' 
    # execute every monday; 
    # minute(0-59), hour(0-23), month day(1-31), week day (0-6)
) as dag:
    
    # breakpoint()

    working_path = f"{file_path}/week={start_date.strftime('%Y-%m-%d')}"

    task_1 = BashOperator(
        task_id="create_folder",
        bash_command=f'mkdir -p {working_path}'
    )

    task_2 = PythonOperator(
        task_id= "extract_data",
        python_callable= extract_data,
        op_kwargs = {
            'working_path': working_path,
            'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d)}}'
        }
    )

    task_1 >> task_2