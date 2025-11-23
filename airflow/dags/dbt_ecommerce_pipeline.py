from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/ecommerce_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"
DBT_RUNTIME_DIR = "/tmp/dbt_runtime"


def dbt_command(subcommand: str) -> str:
    """
    Copia o projeto dbt para um diretório temporário
    e executa um comando específico do dbt.
    """
    return f"""
    rm -rf {DBT_RUNTIME_DIR} && \
    mkdir -p {DBT_RUNTIME_DIR} && \
    cp -r {DBT_PROJECT_DIR} {DBT_RUNTIME_DIR}/ecommerce_dbt && \
    cd {DBT_RUNTIME_DIR}/ecommerce_dbt && \
    dbt {subcommand} \
      --project-dir {DBT_RUNTIME_DIR}/ecommerce_dbt \
      --profiles-dir {DBT_PROFILES_DIR}
    """


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="dbt_ecommerce_pipeline",
    default_args=default_args,
    description="Pipeline Bronze -> Silver -> Gold -> Tests",
    schedule_interval="*/5 * * * *",  # a cada 5 minutos
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "dbt", "snowflake"],
):

    # -------------------------------
    # 1) LOAD BRONZE
    # -------------------------------
    load_bronze = SnowflakeOperator(
        task_id="load_bronze_events",
        snowflake_conn_id="snowflake_default",
        sql="""
            USE DATABASE ECOMMERCE;
            USE SCHEMA BRONZE;

            COPY INTO EVENTS (
                EVENT_ID,
                EVENT_TYPE,
                EVENT_TS,
                USER_ID,
                SESSION_ID,
                PAYLOAD,
                INSERTED_AT
            )
            FROM (
                SELECT
                    $1:event_id::string,
                    $1:event_type::string,
                    $1:event_ts::timestamp_ntz,
                    $1:user_id::string,
                    $1:session_id::string,
                    $1:payload,
                    CURRENT_TIMESTAMP()
                FROM @ECOMMERCE_STAGE
            )
            FILE_FORMAT = (TYPE = JSON)
            ON_ERROR = 'CONTINUE';
        """,
    )

    # -------------------------------
    # 2) DBT SILVER
    # -------------------------------
    dbt_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=dbt_command("run --select tag:silver"),
    )

    # -------------------------------
    # 3) DBT GOLD
    # -------------------------------
    dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=dbt_command("run --select tag:gold"),
    )

    # -------------------------------
    # 4) DBT TESTS
    # -------------------------------
    dbt_tests = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_command("test"),
    )

    # -------------------------------
    # ORDEM FINAL DO PIPELINE
    # -------------------------------
    load_bronze >> dbt_silver >> dbt_gold >> dbt_tests