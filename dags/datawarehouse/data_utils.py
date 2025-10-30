#create the helper funciton
#here we will have the most funcitons relating to database connections and operations like creating tables
#and schemas


#--The first step is to import the Postgres  hook which can be done with the following import statement.

from airflow.providers.postgres.hooks.postgres import PostgresHook

#To interact with our postgre databasae using python. we will be using the Psycopg2

from pyscopg2.extras import RealDictCursor #this will allow retrieval of reports using python dictionaries and not the defailt tuples.

def get_conn_cursor():
    hook=PostgresHook(postgres_conn_id="POSTGRES_DB_YT_ELT",database="elt_db") # 2 args , one is the posgresid form the doker-compose.yaml =>     AIRFLOW_CONN_POSTGRES_DB_YT_ELT: 'postgresql://${ELT_DATABASE_USERNAME}:${ELT_DATABASE_PASSWORD}@${POSTGRES_CONN_HOST}:${POSTGRES_CONN_PORT}/${ELT_DATABASE_NAME}'  the secound is from the .env file => ELT_DATABASE_NAME=elt_db

    conn=hook.get_conn()
    cur=conn.cursor(cursfor_factory=RealDictCursor)
    return conn, cur
    #Ex: cur.execute("select * from table")
def close_conn_cursor(conn,cur):
    cur.close() # to releas resourses
    conn.close()
