table ="yt_api"

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


#build the function to create the schema 
def create_schema(schema):
    conn,cur=get_conn_cursor()

    #create a schema so we can define the following SQL

    schema_sql=f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    conn.commit()#since we are making changes to the database we need to commit these changes.
    close_conn_cursor(conn,cur)
#other function for the tables
def create_table(schema):
    conn,cur=get_conn_cursor()

    if schema=='staging':
        table_sql=f"""
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
            table_sql=f"""
                   CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" TIME NOT NULL,
                    "Video_Type" VARCHAR(10) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT
                );
            """


    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn,cur)



#create a funtion that we should create is to get all the video IDs in either the staging or the correlation
#this will be helpful when we come to loop through the rows of data indide the tables

def get_video_ids(cur,schema):
     cur.execute(f"SELECT Video_ID FROM {schema}.{table};")
     ids=cur.fetchall() #this will give a list of dictinaries where the key is always this variable then the value.
     #Ex: """ 
     #[{'Video_ID':'anksdk'},{'Video_ID':'kjahkfjd'} ...]
     # """


     video_ids=[row["Video_ID"] for row in ids]  
     return video_ids   
    #Ex: """ 
     #['anksdk''kjahkfjd' ...]
     # """



