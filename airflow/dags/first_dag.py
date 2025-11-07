from airflow.models import DAG
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

with DAG(
    'my_first_dag',
    start_date=pendulum.today('UTC').add(days=-2),
    schedule_interval='@daily' # every day at 12pm
) as dag:
    
    task_1 = EmptyOperator(task_id= 'task_1')
    task_2 = EmptyOperator(task_id= 'task_2')
    task_3 = EmptyOperator(task_id= 'task_3')
    
    task_4 = BashOperator(
        task_id= "create_folder",
        bash_command= 'mkdir -p "/home/laryssa/Documents/repositories/apache-airflow/test_{{data_interval_end}}"',
    )

    task_1 >> [task_2, task_3]
    task_3 >> task_4