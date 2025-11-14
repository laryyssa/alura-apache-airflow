from airflow.models import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    'atividade_aula_4',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule_interval='@daily' # every day at 12pm
) as dag:
    
    def function_hello_world():
        print("hello world!")

    task_print = PythonOperator(
        task_id='print_task',
        python_callable=function_hello_world
    )