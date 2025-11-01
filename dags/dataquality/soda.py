import logging
from airflow.operators.bash import BashOperator

logger=logging.getLogger(__name__)
#The global variables for the parts where we will have the YAML files for the checks and configurations and the source
SODA_PATH="/opt/airflow/include/soda"
DATASOURCE="pg_datasource"

def yt_elt_data_quality(schema):
    try:
        task = BashOperator(
            task_id=f"soda_test_schema_{schema}",
            bash_command=f"soda scan -d {DATASOURCE} -c {SODA_PATH}/configuration.yml -v SCHEMA={schema} {SODA_PATH}/checks.yml",
        )
        return task
    except Exception as e:
        logger.error(f"Error running data quality check for schema: {schema}")
        raise e