from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

default_args = {
    'owner': 'seu_nome',
    'depends_on_past': False,
}

@dag(
    dag_id='teste_de_conexoes_etl',
    default_args=default_args,
    description='Testa as conexões com o SQL Server e Postgres',
    schedule=None,
    start_date=datetime(2025, 11, 1),
    tags=['faculdade', 'etl', 'teste'],
)
def connection_test_workflow():

    @task
    def test_sql_server():
        log.info("Tentando conectar ao SQL Server (AdventureWorks)...")
        try:
            mssql_hook = MsSqlHook(mssql_conn_id='oltp_sqlserver')
            mssql_hook.test_connection()
            log.info("Conexão com o SQL Server (AdventureWorks) bem-sucedida!")
            return True
        except Exception as e:
            log.error(f"Falha na conexão com o SQL Server.")
            log.error(e)
            raise e

    @task
    def test_postgres_dw():
        log.info("Tentando conectar ao Data Warehouse (PostgreSQL)...")
        try:
            pg_hook = PostgresHook(postgres_conn_id='dw_postgres')
            pg_hook.test_connection()
            log.info("Conexão com o Data Warehouse (PostgreSQL) bem-sucedida!")
            return True
        except Exception as e:
            log.error(f"Falha na conexão com o PostgreSQL.")
            log.error(e)
            raise e

    [test_sql_server(), test_postgres_dw()]

connection_test_workflow()