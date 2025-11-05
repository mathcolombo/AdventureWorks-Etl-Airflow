from __future__ import annotations

import logging
from datetime import datetime
import pandas as pd

# Importações completas do Airflow
from airflow.sdk import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# IDs das conexões que você criou na UI do Airflow
MSSQL_CONN_ID = "oltp_sqlserver"
POSTGRES_CONN_ID = "dw_postgres"


# --- Funções de ETL (Agora como Tarefas) ---

@task(task_id="etl_dim_tempo")
def etl_dim_tempo():
    """Gera a dimensão de tempo e a carrega no DW."""
    log.info("Gerando dados para dimtempo...")
    
    start_date = '2011-01-01'
    end_date = '2025-12-31'
    dates = pd.date_range(start=start_date, end=end_date)

    df_tempo = pd.DataFrame(dates, columns=['data'])
    df_tempo['tempo_id'] = df_tempo['data'].dt.strftime('%Y%m%d').astype(int)
    df_tempo['ano'] = df_tempo['data'].dt.year
    df_tempo['trimestre'] = df_tempo['data'].dt.quarter
    df_tempo['mes'] = df_tempo['data'].dt.month
    df_tempo['nomemes'] = df_tempo['data'].dt.strftime('%B')
    df_tempo['dia'] = df_tempo['data'].dt.day
    df_tempo['diasemana'] = df_tempo['data'].dt.dayofweek + 1
    df_tempo['nomediasemana'] = df_tempo['data'].dt.strftime('%A')
    df_tempo['fimsemana'] = df_tempo['diasemana'].isin([6, 7])
    
    log.info("Geração de dimtempo concluída. Carregando no DW...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    df_tempo.to_sql('dimtempo', engine, if_exists='replace', index=False)
    
    log.info("Carga de dimtempo concluída.")
    return 'dimtempo'


@task(task_id="etl_dim_cliente")
def etl_dim_cliente():
    """Extrai, transforma e carrega a dimensão de cliente."""
    log.info("Extraindo e transformando dados para dimcliente...")
    
    query = """
    SELECT DISTINCT
        c.CustomerID AS cliente_id, p.FirstName, p.LastName,
        ea.EmailAddress AS email, pp.PhoneNumber AS telefone,
        p.PersonType AS tipopessoa
    FROM Sales.Customer AS c
    JOIN Person.Person AS p ON c.PersonID = p.BusinessEntityID
    LEFT JOIN Person.EmailAddress AS ea ON c.PersonID = ea.BusinessEntityID
    LEFT JOIN Person.PersonPhone AS pp ON c.PersonID = pp.BusinessEntityID;
    """
    
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    df_cliente = mssql_hook.get_pandas_df(query)
    
    # Transformação
    df_cliente['nomecompleto'] = df_cliente['FirstName'] + ' ' + df_cliente['LastName']
    df_dim_cliente = df_cliente[['cliente_id', 'nomecompleto', 'email', 'telefone', 'tipopessoa']]
    
    log.info("Transformação de dimcliente concluída. Carregando no DW...")
    
    # Carga
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    df_dim_cliente.to_sql('dimcliente', engine, if_exists='replace', index=False)
    
    log.info("Carga de dimcliente concluída.")
    return 'dimcliente'


@task(task_id="etl_dim_produto")
def etl_dim_produto():
    """Extrai, transforma e carrega a dimensão de produto."""
    log.info("Extraindo e transformando dados para dimproduto...")
    
    query = """
    SELECT DISTINCT
        p.ProductID AS produto_id, p.Name AS nome, p.ProductNumber AS numero,
        p.Color AS cor, psc.Name AS subcategoria, pc.Name AS categoria
    FROM Production.Product AS p
    LEFT JOIN Production.ProductSubcategory AS psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
    LEFT JOIN Production.ProductCategory AS pc ON psc.ProductCategoryID = pc.ProductCategoryID;
    """
    
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    df_produto = mssql_hook.get_pandas_df(query)
    df_dim_produto = df_produto[['produto_id', 'nome', 'numero', 'cor', 'subcategoria', 'categoria']]
    
    log.info("Transformação de dimproduto concluída. Carregando no DW...")
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    df_dim_produto.to_sql('dimproduto', engine, if_exists='replace', index=False)
    
    log.info("Carga de dimproduto concluída.")
    return 'dimproduto'


@task(task_id="etl_dim_localizacao")
def etl_dim_localizacao():
    """Extrai, transforma e carrega a dimensão de localização."""
    log.info("Extraindo e transformando dados para dimlocalizacao...")
    
    query = """
    SELECT DISTINCT
        a.AddressID AS localizacao_id, sp.CountryRegionCode AS codigopaisregiao,
        sp.Name AS provinciaestado, a.City AS cidade, a.PostalCode AS codigopostal
    FROM Person.Address AS a
    JOIN Person.StateProvince AS sp ON a.StateProvinceID = sp.StateProvinceID;
    """
    
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    df_localizacao = mssql_hook.get_pandas_df(query)
    df_dim_localizacao = df_localizacao[['localizacao_id', 'codigopaisregiao', 'provinciaestado', 'cidade', 'codigopostal']]

    log.info("Transformação de dimlocalizacao concluída. Carregando no DW...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    df_dim_localizacao.to_sql('dimlocalizacao', engine, if_exists='replace', index=False)

    log.info("Carga de dimlocalizacao concluída.")
    return 'dimlocalizacao'


@task(task_id="etl_dim_vendedor")
def etl_dim_vendedor():
    """Extrai, transforma e carrega a dimensão de vendedor."""
    log.info("Extraindo e transformando dados para dimvendedor...")
    
    query = """
    SELECT DISTINCT
        sp.BusinessEntityID AS vendedor_id, p.FirstName, p.LastName,
        e.JobTitle AS cargo, e.Gender AS genero
    FROM Sales.SalesPerson AS sp
    JOIN HumanResources.Employee AS e ON sp.BusinessEntityID = e.BusinessEntityID
    JOIN Person.Person AS p ON e.BusinessEntityID = p.BusinessEntityID;
    """
    
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    df_vendedor = mssql_hook.get_pandas_df(query)
    df_vendedor['nomecompleto'] = df_vendedor['FirstName'] + ' ' + df_vendedor['LastName']
    df_dim_vendedor = df_vendedor[['vendedor_id', 'nomecompleto', 'cargo', 'genero']]
    
    log.info("Transformação de dimvendedor concluída. Carregando no DW...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    df_dim_vendedor.to_sql('dimvendedor', engine, if_exists='replace', index=False)

    log.info("Carga de dimvendedor concluída.")
    return 'dimvendedor'


@task(task_id="etl_fact_vendas")
def etl_fact_vendas():
    """
    Extrai, transforma e carrega (substituindo) a tabela de fatos de vendas.
    A lógica de TRUNCATE foi movida para cá.
    """
    log.info("Extraindo e transformando dados para factvendas...")
    
    query = """
    SELECT
        soh.OrderDate, soh.CustomerID AS cliente_id, sod.ProductID AS produto_id,
        sod.OrderQty AS quantidadevendida, sod.UnitPrice AS precounitario,
        sod.UnitPriceDiscount AS desconto, sod.LineTotal AS valortotalvenda,
        soh.SalesPersonID AS vendedor_id, fa.AddressID AS faturalocalizacao_id,
        sa.AddressID AS entregalocalizacao_id
    FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod ON soh.SalesOrderID = sod.SalesOrderID
    JOIN Person.Address AS fa ON soh.BillToAddressID = fa.AddressID
    JOIN Person.Address AS sa ON soh.ShipToAddressID = sa.AddressID
    WHERE soh.OrderDate IS NOT NULL
    """
    
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    df_vendas = mssql_hook.get_pandas_df(query)
    
    # Transformação
    df_vendas['custototalproduto'] = (df_vendas['precounitario'] * (1 - df_vendas['desconto'])) * df_vendas['quantidadevendida']
    df_vendas['tempo_id'] = pd.to_datetime(df_vendas['OrderDate']).dt.strftime('%Y%m%d').astype(int)
    
    df_fact_vendas = df_vendas[[
        'tempo_id', 'cliente_id', 'produto_id', 'vendedor_id', 'faturalocalizacao_id',
        'entregalocalizacao_id', 'quantidadevendida', 'precounitario', 'desconto',
        'valortotalvenda', 'custototalproduto'
    ]]
    
    log.info("Transformação de factvendas concluída. Carregando no DW...")

    # Carga
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Usamos 'replace' para a fato. Isso faz o TRUNCATE e o LOAD
    # em uma única operação atômica.
    df_fact_vendas.to_sql('factvendas', engine, if_exists='replace', index=False)
    
    log.info("Carga de factvendas concluída.")
    return 'factvendas'


# --- Definição do DAG ---

@dag(
    dag_id='etl_adventureworks_para_dw',
    description='ETL completo do AdventureWorks para o Data Warehouse PostgreSQL',
    schedule=None,
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['faculdade', 'etl', 'completo'],
)
def full_etl_workflow():
    """
    Este DAG orquestra o ETL completo.
    
    1. Carrega todas as dimensões em paralelo.
    2. Carrega a tabela de fatos *depois* que todas as dimensões terminarem.
    """
    
    # 6. Cria uma lista com todas as tarefas de dimensão
    dim_tasks = [
        etl_dim_tempo(),
        etl_dim_cliente(),
        etl_dim_produto(),
        etl_dim_localizacao(),
        etl_dim_vendedor()
    ]
    
    # A tarefa de carga da fato é chamada aqui
    carga_fato = etl_fact_vendas()
    
    # 7. Define a ordem:
    # Todas as tarefas em 'dim_tasks' devem terminar
    # ANTES da 'carga_fato' começar.
    dim_tasks >> carga_fato

# Instancia o DAG (necessário para o Airflow encontrá-lo)
full_etl_workflow()