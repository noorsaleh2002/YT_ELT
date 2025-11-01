table = "yt_api"

# create the helper functions
# here we will have the most functions relating to database connections and operations like creating tables
# and schemas

# --The first step is to import the Postgres hook which can be done with the following import statement.
from airflow.providers.postgres.hooks.postgres import PostgresHook

# To interact with our postgre database using python. we will be using the Psycopg2
import psycopg2
from psycopg2.extras import RealDictCursor  # this will allow retrieval of reports using python dictionaries and not the default tuples.

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="POSTGRES_DB_YT_ELT", database="elt_db")  # 2 args , one is the postgresid from the docker-compose.yaml =>     AIRFLOW_CONN_POSTGRES_DB_YT_ELT: 'postgresql://${ELT_DATABASE_USERNAME}:${ELT_DATABASE_PASSWORD}@${POSTGRES_CONN_HOST}:${POSTGRES_CONN_PORT}/${ELT_DATABASE_NAME}'  the second is from the .env file => ELT_DATABASE_NAME=elt_db
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur
    # Ex: cur.execute("select * from table")

def close_conn_cursor(conn, cur):
    cur.close()  # to release resources
    conn.close()

# build the function to create the schema 
def create_schema(cur, schema):
    # create a schema so we can define the following SQL
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    # Note: No commit here as the calling function should handle commits

# other function for the tables
def create_table(cur, schema, table):
    if schema == 'staging':
        table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT
                    );
            """
    else:
        table_sql = f"""
                   CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" INT NOT NULL,
                    "Video_Type" VARCHAR(10) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT
                );
            """
    cur.execute(table_sql)
    # Note: No commit here as the calling function should handle commits

# create a function that we should create is to get all the video IDs in either the staging or the correlation
# this will be helpful when we come to loop through the rows of data inside the tables
def get_video_ids(cur, schema, table):
    cur.execute(f'SELECT "Video_ID" FROM {schema}.{table};')
    ids = cur.fetchall()  # this will give a list of dictionaries where the key is always this variable then the value.
    # Ex: """ 
    # [{'Video_ID':'anksdk'},{'Video_ID':'kjahkfjd'} ...]
    # """
    video_ids = [row["Video_ID"] for row in ids]  
    return video_ids   
    # Ex: """ 
    # ['anksdk''kjahkfjd' ...]
    # """