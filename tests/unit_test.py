# """
# Going back to the unit test Python script, first we create the function which as we said, needs to

# start with test underscore and then give it an appropriate name which is API underscore key.

# This function will need to take as argument the API underscore key fixture we defined in the conf test.py.

# So let's start by writing test underscore API underscore key which takes API underscore key argument.

# Then to verify the API key, we simply use the Python assert keyword to test if API underscore key is

# in fact equal to what we defined in the fixture, which should be mock underscore key 1234.

# Before we check if the test passes or fails.

# A quick note that I want to add is that apart from naming conventions for functions, Pytest also has

# naming conventions for the script names.

# So we name the script unit underscore test because Pytest requires that the Python script starts with

# test underscore or ends in underscore test.

# And here we are using the latter letter syntax.

# So going back to the results of our test.

# Let's go inside one of the airflow containers.

# Here we already are inside the airflow worker container.

# And let's run our test.

# So we write Pytest dash v.

# We use the dash v for a more verbose output, and then we define the path of where the unit test script

# is, which is tests unit underscore test py for now.

# To run this script, it would give the output for the single function.

# However, we will continue to build more tests.

# So to specify a particular function we use the dash k option with the function name.

# Let's run it.


# """
# # tests/unit_test.py
def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"





def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHEES"




def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password" 
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"

def test_dags_integrity(dagbag):
    # 1. Check for import errors
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("---")
    print(dagbag.import_errors)

    # 2. Check expected DAGs are present
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]  # Fixed: produce.json to produce_json
    loaded_dag_ids = list(dagbag.dags.keys())
    print("---")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing."  # Fixed f-string syntax

    # 3. Check total number of DAGs
    assert dagbag.size() == 3
    print("---")
    print(dagbag.size())

    # 4. Check task counts for each DAG
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2,
    }

    print("========")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)
        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."  # Fixed f-string syntax
        print(dag_id, len(dag.tasks))




