
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'download_csse',
    default_args=default_args,
    description='Download and preprocess CSSE data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 12, 20),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='git_pull',
        bash_command='cd ~/COVID-19 && git pull',
    )

    t2 = BashOperator(
        task_id='merge_csv',
        bash_command='cd ~/covid-19-stats && . ~/.virtualenvs/covid-19-stats-load/bin/activate && python -m covid19stats.csse_load',
    )

    t1 >> t2


with DAG(
    'download_cdc_deaths_2018',
    default_args=default_args,
    description='Download CDC Deaths data for 2018',
    schedule_interval=timedelta(days=30),
    start_date=datetime(2021, 12, 20),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='download',
        bash_command='cd ~/covid-19-stats && . ~/.virtualenvs/covid-19-stats-load/bin/activate && python -m covid19stats.cdc_deaths_2018_download',
    )


with DAG(
    'download_cdc_deaths_2020',
    default_args=default_args,
    description='Download CDC Deaths data for 2020',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2021, 12, 20),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='download',
        bash_command='cd ~/covid-19-stats && . ~/.virtualenvs/covid-19-stats-load/bin/activate && python -m covid19stats.cdc_deaths_2020_download',
    )


with DAG(
    'download_cdc_states',
    default_args=default_args,
    description='Download CDC states data',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2021, 12, 20),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='download',
        bash_command='cd ~/covid-19-stats && . ~/.virtualenvs/covid-19-stats-load/bin/activate && python -m covid19stats.cdc_states_download',
    )
