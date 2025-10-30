import logging

logger=logging.getLogger(__name__)
table='yt_api'

# create a function called insert worw that will insert the api data , which we load it from the json file into the staging and core tables
#this function will take the cursor and connection objects , the scjema tp differemtiate between staging and the core


def insert_rows(cur, conn, schema, table, row):
        try:
            if schema == 'staging':
                video_id = 'video_id'
                cur.execute(
                    f"""INSERT INTO {schema}.{table} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                    VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)""", 
                    row
                )
            else:
                video_id = 'Video_ID'
                #here we donot want the json , we are not longer reading from the json like we did for staging for the core layer , we need data from the previous layer , which is the staging layer

                cur.execute(
                f"""INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)""", 
                row
                )

            conn.commit()
            logger.info(f"Inserted row with Video_ID : {row[video_id]}")
        except Exception as e:
            logger.error(e)
            raise e
def update_rows(cur, conn, schema, table, row):
    try:
        # staging
        if schema == 'staging':
            video_id = 'video_id'
            upload_date = 'publishedAt'
            video_title = 'title'
            video_views = 'viewCount'
            likes_count = 'likeCount'
            comments_count = 'commentCount'
        # core
        else:
            video_id = 'Video_ID'
            upload_date = 'Upload_Date'
            video_title = 'Video_Title'
            video_views = 'Video_Views'
            likes_count = 'Likes_Count'
            comments_count = 'Comments_Count'

        cur.execute(
            f""" 
            UPDATE {schema}.{table}
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s,
                "Likes_Count" = %({likes_count})s,
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """, row
        )
        conn.commit()
        logger.info(f"Updated row with Video_ID:{row[video_id]}")
    except Exception as e:
        print(f"Error updating rows: {e}")
        raise e
          
def delete_rows(cur, conn, schema, table, lds_to_delete):
    try:
        lds_to_delete_formatted = f"""{', '.join(f"'{id}'" for id in lds_to_delete)}"""
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN ({lds_to_delete_formatted});
            """
        )
        conn.commit()
        logger.info(f'Deleted rows with Video_IDs: {lds_to_delete}')
    except Exception as e:
        logger.error(f'Error deleting rows with Video_IDs: {lds_to_delete} - {e}')
        raise e
