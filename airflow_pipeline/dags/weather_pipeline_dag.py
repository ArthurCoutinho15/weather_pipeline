from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def execute_script():
    import subprocess
    subprocess.run(['python','/home/arthur/weather_pipeline/src/extract.py'])

with DAG(dag_id='dados_climaticos', start_date=days_ago(1), schedule_interval='@daily') as dag:
    task_1 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable= execute_script
    )