FROM apache/airflow:3.1.1

USER root

RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc-dev \
    build-essential

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

RUN curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

RUN apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /
RUN pip install --user -r /requirements.txt