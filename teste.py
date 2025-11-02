# Importe as bibliotecas necessárias
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd  # Exemplo, se você usar pandas para transformar

# --- 1. Definições Padrão ---
# Estes argumentos serão aplicados a todas as tarefas
default_args = {
    'owner': 'seu_nome',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# --- 2. Definição do DAG ---
# @dag é um "decorator" que transforma a função em um DAG
@dag(
    dag_id='processo_etl_faculdade',
    default_args=default_args,
    description='Meu primeiro DAG de ETL para a faculdade',
    # 'schedule_interval=None' significa que só roda manualmente
    # Você poderia usar '@daily' ou um 'cron' '0 5 * * *'
    schedule=None,
    start_date=datetime(2025, 11, 1),  # Data de início (ontem)
    tags=['faculdade', 'etl', 'exemplo'],
)
def etl_workflow():
    """
    Este é o fluxo principal do DAG, definido como uma função.
    """

    # --- 3. Definição das Tarefas (Tasks) ---
    # @task transforma uma função Python em uma Tarefa do Airflow
    
    @task
    def extract() -> pd.DataFrame:
        """
        Tarefa de Extração (E).
        Simula a extração de dados e retorna um DataFrame pandas.
        """
        print("Iniciando extração...")
        # NO SEU TRABALHO: Aqui você se conectaria ao banco, leria um CSV, etc.
        data = {
            'id': [1, 2, 3],
            'valor_bruto': [100, 200, 300]
        }
        df = pd.DataFrame(data)
        print(f"Extraiu {len(df)} linhas.")
        return df

    @task
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        """
        Tarefa de Transformação (T).
        Recebe o DataFrame da tarefa anterior e aplica uma regra.
        """
        print("Iniciando transformação...")
        # NO SEU TRABALHO: Aqui você faria suas lógicas de negócio, joins, etc.
        df['imposto'] = df['valor_bruto'] * 0.1
        df['valor_liquido'] = df['valor_bruto'] - df['imposto']
        print("Transformação concluída.")
        return df

    @task
    def load(df: pd.DataFrame):
        """
        Tarefa de Carga (L).
        Recebe o DataFrame transformado e o "carrega" em algum lugar.
        """
        print("Iniciando carga...")
        # NO SEU TRABALHO: Aqui você salvaria no Data Warehouse, em outro CSV, etc.
        print("Dados a carregar:")
        print(df)
        print("Carga concluída.")
        return True

    # --- 4. Definição das Dependências (A Ordem) ---
    # O Airflow entende a ordem automaticamente pela forma como
    # você "chama" as funções e passa os resultados.
    
    dados_extraidos = extract()
    dados_transformados = transform(dados_extraidos)
    load(dados_transformados)

# --- 5. Instancie o DAG ---
# Isso é o que o Airflow procura no arquivo.
etl_workflow()