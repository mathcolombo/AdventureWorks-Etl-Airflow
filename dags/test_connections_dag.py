from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

# 1. Importe os "Hooks" corretos
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# Argumentos padrão para o DAG
default_args = {
    'owner': 'seu_nome',
    'depends_on_past': False,
}

@dag(
    dag_id='teste_de_conexoes_etl',
    default_args=default_args,
    description='Testa as conexões com o SQL Server e Postgres',
    # 'schedule=None' significa que só roda manualmente
    schedule=None, 
    start_date=datetime(2025, 11, 1), # Use uma data fixa no passado
    tags=['faculdade', 'etl', 'teste'],
)
def connection_test_workflow():
    """
    Este DAG contém duas tarefas para testar as conexões do banco de dados.
    """

    @task
    def test_sql_server():
        """
        Testa a conexão com o SQL Server (AdventureWorks).
        """
        log.info("Tentando conectar ao SQL Server (AdventureWorks)...")
        try:
            # 2. Use o Conn Id que você criou na UI do Airflow
            mssql_hook = MsSqlHook(mssql_conn_id='oltp_sqlserver')
            
            # 3. Chame o método de teste
            mssql_hook.test_connection()
            
            log.info("Conexão com o SQL Server (AdventureWorks) bem-sucedida!")
            return True
        except Exception as e:
            log.error(f"Falha na conexão com o SQL Server.")
            log.error(e)
            raise e # Força a tarefa a falhar para você ver o erro

    @task
    def test_postgres_dw():
        """
        Testa a conexão com o Data Warehouse (PostgreSQL).
        """
        log.info("Tentando conectar ao Data Warehouse (PostgreSQL)...")
        try:
            # 2. Use o Conn Id que você criou para o Postgres
            # (Eu assumi o nome 'meu_dw_postgres', mude se for diferente)
            pg_hook = PostgresHook(postgres_conn_id='dw_postgres')
            
            # 3. Chame o método de teste
            pg_hook.test_connection()
            
            log.info("Conexão com o Data Warehouse (PostgreSQL) bem-sucedida!")
            return True
        except Exception as e:
            log.error(f"Falha na conexão com o PostgreSQL.")
            log.error(e)
            raise e # Força a tarefa a falhar

    # 4. Define a ordem (podem rodar em paralelo)
    [test_sql_server(), test_postgres_dw()]

# Instancia o DAG
connection_test_workflow()