"""Выгрузка из бонусной системы в DWH"""
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.samba.hooks.samba import SambaHook
from roskadastr.logger import logger
import mmap
import io

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

SMB_BASEDIR = '/public/Управление информационных технологий/Отдел разработки программного обеспечения/_ежедневка_2022'

def get_from_samba():
    samba_hook = SambaHook(samba_conn_id='smb_public')
    result = []
    dt = '03.03.2023'
    dirs = samba_hook.listdir(f"/{dt}")
    for dir in dirs:
        if 'zip' in dir:
            src = f"{SMB_BASEDIR}/{dt}/{dir}"
            dest = f"/data/{dir}"
            with open(dest, 'wb') as local_file:
                with samba_hook.open_file(src, buffering=65536, mode="rb") as smb_file:
                    chunk = smb_file.read(65536)
                    while chunk:
                        local_file.write(chunk)
                        chunk = smb_file.read(1024)
            result.append(dest)


with DAG(
        'request-to-pg-dag',
        catchup=False,
        default_args=args,
        description='Dag for load data from origin bonus system to staging',
        is_paused_upon_creation=True,
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        schedule_interval='*/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
        tags=['sprint5', 'stg', 'origin'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    read_from_samba_task = PythonOperator(
        task_id='read_from_samba_task',
        python_callable=get_from_samba,
        dag=dag
    )

    start >> read_from_samba_task >> end
