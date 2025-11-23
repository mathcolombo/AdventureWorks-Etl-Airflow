# 📖 Guia de Configuração: ETL com Airflow, Docker e SQL Server

Este documento detalha o processo passo a passo para configurar um ambiente de desenvolvimento Airflow usando Docker, capaz de se conectar a um banco de dados SQL Server local (OLTP) e a um PostgreSQL (Data Warehouse).

---

## 1. 🐳 Configuração do Ambiente Docker & Airflow

O núcleo do nosso setup é o Docker, que garante que o Airflow rode em um ambiente Linux controlado. No entanto, a imagem oficial não vem com os drivers específicos para o SQL Server.

### Passo 1: Obter o Docker Compose

Faça o download do arquivo docker-compose.yaml oficial do Airflow. Este arquivo descreve todos os serviços necessários (webserver, scheduler, postgres, redis, etc.).

Link: https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml

### Passo 2: Criar um Dockerfile para o Driver ODBC

Para que o Airflow (Linux) possa falar com o SQL Server (Windows), ele precisa de um driver ODBC específico. Criamos um Dockerfile (no mesmo diretório do docker-compose.yaml) para construir uma imagem customizada do Airflow.

    # Começa com a imagem oficial do Airflow
    FROM apache/airflow:latest

    # Muda para o usuário 'root' para instalar pacotes
    USER root

    # Instala as dependências do sistema para o driver ODBC
    RUN apt-get update && apt-get install -y \
        curl \
        gnupg \
        unixodbc-dev \
        build-essential

    # Adiciona a chave e o repositório da Microsoft
    RUN curl [https://packages.microsoft.com/keys/microsoft.asc](https://packages.microsoft.com/keys/microsoft.asc) | apt-key add -
    RUN curl [https://packages.microsoft.com/config/debian/12/prod.list](https://packages.microsoft.com/config/debian/12/prod.list) > /etc/apt/sources.list.d/mssql-release.list

    # Instala o driver (msodbcsql17)
    RUN apt-get update && \
        ACCEPT_EULA=Y apt-get install -y msodbcsql17

    # Limpa o cache
    RUN apt-get clean && rm -rf /var/lib/apt/lists/*

    # Devolve para o usuário 'airflow'
    USER airflow

    # Copia e instala as dependências Python
    COPY requirements.txt /
    RUN pip install --user -r /requirements.txt

### Passo 3: Criar o requirements.txt

Este arquivo diz ao Dockerfile quais bibliotecas Python (provedores) instalar. É essencial para que o Airflow tenha os "Hooks" do MS SQL e Postgres, além do Pandas.

    pandas
    apache-airflow-providers-microsoft-mssql
    apache-airflow-providers-postgres


### Passo 4: Modificar o docker-compose.yaml

Editamos o docker-compose.yaml para que ele construa (build) nossa imagem customizada em vez de baixar a imagem oficial.

Encontramos o bloco x-airflow-common: e fizemos a seguinte alteração:

    x-airflow-common:
    &airflow-common
    # ... (comentários)
    # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.1.1}  <-- COMENTAMOS ESTA LINHA
    build: .                                        <-- DESCOMENTAMOS ESTA LINHA
    # ... (resto do bloco)


Passo 5: Iniciar o Ambiente

Com os 3 arquivos (docker-compose.yaml, Dockerfile, requirements.txt) na mesma pasta, subimos o ambiente pela primeira vez.

    docker-compose up --build -d


**💡 Ponto-chave: O --build força o Docker a construir a imagem customizada com nossos drivers, um processo que só precisa ser feito uma vez (ou quando os requisitos mudam).**

## 2. 🗄️ Configuração dos Bancos de Dados

O Airflow (dentro do Docker) precisa acessar dois bancos de dados que estão rodando na nossa máquina (o "host").

### SQL Server (OLTP - A Fonte)

O SQL Server precisou de duas configurações críticas para aceitar conexões vindas do Docker:

#### A. Habilitar Autenticação SQL (Modo Misto)

O Docker (Linux) não pode usar a "Autenticação do Windows" (Trusted_Connection).

No SQL Server Management Studio (SSMS), clicamos com o botão direito no servidor > Propriedades > Segurança.

Mudamos a autenticação para "Modo de Autenticação do SQL Server e do Windows".

Reiniciamos o serviço do SQL Server.

#### B. Criar um Usuário SQL

Em Segurança > Logons, criamos um novo logon (ex: airflow_user com uma senha).

Desmarcamos "Impor política de senha" (para simplificar).

Em Mapeamento de Usuário, demos a ele permissão de db_datareader no banco AdventureWorks2019.

#### C. Habilitar Conexões de Rede (TCP/IP)

Este foi o passo que resolveu o erro Connection refused (111).

Abrimos o SQL Server Configuration Manager.

Fomos em Configuração de Rede do SQL Server > Protocolos para MSSQLSERVER.

Habilitamos o protocolo TCP/IP.

Nas propriedades do TCP/IP, aba IP Addresses, rolamos até IPAll e garantimos que a Porta TCP estava 1433 e que as Portas Dinâmicas TCP estavam em branco.

Reiniciamos o serviço do SQL Server novamente.

### PostgreSQL (DW - O Destino)

Este banco de dados já estava configurado para aceitar conexões por usuário/senha (feito no primeiro trabalho), então nenhuma ação extra foi necessária.

## 3. 🌐 Configuração das Conexões no Airflow

Com os bancos prontos, ensinamos o Airflow a encontrá-los. Na UI do Airflow (http://localhost:8080), fomos em Admin -> Connections:

### Conexão 1: PostgreSQL (DW)

    Conn Id: dw_postgres
    Conn Type: Postgres
    Host: host.docker.internal
    Login: sis_etl
    Password: *
    Port: 5432
    Schema: datawarehouse

### Conexão 2: SQL Server (OLTP)

    Conn Id: oltp_sqlserver
    Conn Type: MS SQL
    Host: host.docker.internal
    Login: airflow_user
    Password: *
    Port: 1433
    Extra: (Deixamos VAZIO. Adicionar {"driver": ...} causou um TypeError).

**💡 Ponto-chave: host.docker.internal é o "apelido" de rede especial que o Docker usa para se referir à máquina que está hospedando o container.**

## 4. 💻 Configuração do Ambiente Local (VS Code)

Para que o VS Code e o Pylance parassem de mostrar erros de importação (could not be resolved), criamos um ambiente virtual local que "espelha" as dependências do Docker.

Criar o ambiente:

    python -m venv .venv


Ativar o ambiente:

    # (Windows)
    .\.venv\Scripts\Activate.ps1


Instalar TODAS as dependências:

    # Instala o Airflow base (para os decoradores @dag, @task)
    pip install apache-airflow
    # Instala os mesmos pacotes do Docker
    pip install -r requirements.txt


Selecionar o Interpretador: No VS Code, usamos Ctrl+Shift+P > Python: Select Interpreter e apontamos para o Python dentro da pasta .venv.

## 5. 🐞 Guia de Solução de Problemas (Erros Corrigidos)

Durante o processo, encontramos e corrigimos vários erros:

### Internal Server Error
**Erro**: Internal Server Error (500) na UI do Airflow após o build.
**Causa**: A tabela de sessão do usuário no banco de metadados do Airflow estava corrompida ou incompatível após a atualização.
**Solução**: Limpar a tabela de sessão

    docker exec -it [nome-do-container-postgres] psql -U airflow -d airflow

    TRUNCATE TABLE session;

    \q

    docker-compose restart airflow-apiserver

### Não admin
**Erro**: Não conseguia acessar Admin -> Connections ("Não admin").
**Causa**: O usuário airflow perdeu a associação com a função Admin após o build.
**Solução**: Readicionar a função manualmente.

    docker-compose exec airflow-scheduler airflow users add-role --username airflow --role Admin

### TypeError
**Erro**: TypeError: connect() got an unexpected keyword argument 'driver'.
**Causa**: O campo "Extra" da conexão oltp_sqlserver continha um JSON ({"driver": ...}).
**Solução**: Editar a conexão e deixar o campo "Extra" vazio.

### OperationalError
**Erro**: OperationalError: ... Connection refused (111).
**Causa**: O SQL Server não estava aceitando conexões de rede (TCP/IP).
**Solução**: Seguir o Passo 2C e habilitar o TCP/IP no SQL Server Configuration Manager.