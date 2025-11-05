# 1. Comece com a imagem oficial do Airflow que você está usando
# (Se você baixou recentemente, provavelmente é a 'latest')
FROM apache/airflow:3.1.1

# 2. Mude para o usuário 'root' para poder instalar pacotes
USER root

# 3. Instale os pré-requisitos para o driver ODBC
#    - curl: para baixar os scripts de instalação
#    - gnupg: para verificar as chaves de assinatura
#    - unixodbc-dev: O gerenciador de drivers ODBC para Linux
#    - build-essential: Para compilar o pyodbc
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc-dev \
    build-essential

# 4. Adicione a chave GPG da Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

# 5. Adicione o repositório de pacotes da Microsoft para Debian 12 (Bookworm)
#    (A imagem base do Airflow 'latest' usa Debian 12)
RUN curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

# 6. Atualize a lista de pacotes e instale o driver
#    O 'ACCEPT_EULA=Y' é crucial para a instalação não-interativa
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

# 7. Limpe o cache do apt para manter a imagem pequena
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# 8. Mude de volta para o usuário 'airflow'
USER airflow

# 9. (RECOMENDADO) Copie e instale as dependências Python
COPY requirements.txt /
RUN pip install --user -r /requirements.txt