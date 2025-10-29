import requests
import json
import os
from dotenv import load_dotenv
#using the channel recourses from the documentation
load_dotenv(dotenv_path="./.env")
API_KEY=os.getenv("API_KEY")
CHANNEL_HANDLE=os.getenv("CHANNEL_HANDLE")
def get_playlistId():
    try:

        url=f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        responce=requests.get(url)

        responce.raise_for_status()

        data = responce.json()
        # print(json.dumps(data,indent=4))
        channel_items=data["items"][0]
        channel_playlistId=channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        # print(channel_playlistId)
        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e

#at this stage we have the vasic code to get the palylist id, but we need to do some changes in the code to maje ut nore modular and adhere to software eng best practic ==>create the funciotn 



if __name__ == "__main__":

    get_playlistId()
