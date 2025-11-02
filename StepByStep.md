# Step By Step

----

## Project Configuration 

1. Install the AirFlow [https://airflow.apache.org/docs/apache-airflow/3.1.1/docker-compose.yaml](docker-compose) in the project folder.

2. Start the Python virtual environment:
    ```
    python -m venv .venv
    ```
3. Install the necessary libraries for the project:
    ```
    pip install pandas sqlalchemy psycopg2-binary pyodbc
    ```
4. Start Docker using the installed docker-compose:
    ```
    docker-compose up -d
    ```

