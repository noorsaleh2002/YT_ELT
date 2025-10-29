import requests
import json
import os
from dotenv import load_dotenv
from datetime import date
load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")
MAX_RESULTS = 50

if not API_KEY or not CHANNEL_HANDLE:
    raise ValueError("Missing API_KEY or CHANNEL_HANDLE in .env file")

def get_playlist_id():
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

def get_video_ids(playlist_id):
    video_ids = []
    page_token = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULTS}&playlistId={playlist_id}&key={API_KEY}"
    while True:
        url = base_url + (f"&pageToken={page_token}" if page_token else "")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        video_ids.extend([item["contentDetails"]["videoId"] for item in data.get("items", [])])
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return video_ids

def extract_video_data(video_ids):
    extracted_data = []

    # Generator function for batching
    def batch_list(video_id_list, batch_size):
        for i in range(0, len(video_id_list), batch_size):
            yield video_id_list[i:i + batch_size]  # yields sublists

    try:
        for batch in batch_list(video_ids, MAX_RESULTS):
            # IDs must be comma-separated, not dot-separated
            video_ids_str = ",".join(batch)

            # Combine parts into a single parameter
            url = (
                f"https://youtube.googleapis.com/youtube/v3/videos"
                f"?part=contentDetails,snippet,statistics&id={video_ids_str}&key={API_KEY}"
            )

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item.get('snippet', {})
                content_details = item.get('contentDetails', {})
                statistics = item.get('statistics', {})

                video_data = {
                    "video_id": video_id,
                    "title": snippet.get('title'),
                    "publishedAt": snippet.get('publishedAt'),
                    "duration": content_details.get('duration'),
                    "viewCount": statistics.get('viewCount'),
                    "likeCount": statistics.get('likeCount'),
                    "commentCount": statistics.get('commentCount'),
                }
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

def save_to_json(extracted_data):
    file_path=f"./data/YT_data_{date.today()}.json"
    #using the context manager : with 
    with open(file_path,"w",encoding="utf-8") as json_outfile:
        json.dump(extracted_data,json_outfile,indent=4,ensure_ascii=False)






if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    videos_data = extract_video_data(video_ids)
    save_to_json(videos_data)
    
 
