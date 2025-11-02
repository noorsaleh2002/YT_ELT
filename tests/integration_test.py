import pytest
import requests
import psycopg2

def test_youtube_api_response(airflow_variable):
    api_key=airflow_variable("api_key")
    channel_handle=airflow_variable("channel_handle")
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    #this is the url for the get playlist id
    #it would be better to use the try and eccept , since we are connecting to the acutal real database As there are more potential fail points than when using a mock connection
    #such as network issuse , timeouts , authentication failures , 
    try:
        responce=requests.get(url)
    except requests.RequestException as e:
        pytest.fail(f"Request to YouTube API failed: {e}")
#Another test that we can do is with the connection to the database that will store the ETL data. using the real creadintioal
def test_real_postgres_commection(real_postgres_connection):
    cursor=None
    try:
        cursor=real_postgres_connection.cursor()
        cursor.execute("select 1;")
        result=cursor.fetchone()
        assert result[0]==1  # result is a tuble when created using the cursor.fetchone method
    except psycopg2.Error as e:
        pytest.fail(f"Database query failed: {e}")
    finally:
        if cursor is not None:
            cursor.close()   
