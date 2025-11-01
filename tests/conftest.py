#we will moke the API key
import pytest
from unittest import mock
from airflow.models import Variable,Connection,DagBag

#this function is a fixtrue and will be used to provide the input data for the main test

@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")

@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="MRCHEES"):
        yield Variable.get("CHANNEL_HANDLE")
#Now that we have created unit tests for variable mocking , let's do the same for database connections (where we are storing the API data)
#for this we need to import the connection module
@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        conn_type="postgres",
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name",  # schema is the db name
    )
    conn_url = conn.get_uri()  # Use get_uri() instead of get_url()
    
    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_url):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")

#one final unit test that we can do involves testing with our DAGs are structures as we expect them 
#to interact with dag , we use the dag baf instance which collects all out dags information so we can continue the airflow

@pytest.fixture
def dagbag():
    yield DagBag()