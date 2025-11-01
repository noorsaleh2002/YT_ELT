from datetime import timedelta, datetime

def parse_duration(duration_str):
    duration_str = duration_str.replace("PT", "")
    
    components = ["H", "M", "S"]
    values = {"H": 0, "M": 0, "S": 0}
    
    current_value = ""
    for char in duration_str:
        if char.isdigit():
            current_value += char
        else:
            if char in values:
                values[char] = int(current_value) if current_value else 0
            current_value = ""
    
    total_duration = timedelta(
        hours=values["H"], 
        minutes=values["M"], 
        seconds=values["S"]
    )
    
    return total_duration

def transform_data(row):
    duration_td = parse_duration(row["Duration"])
    
    # Create a new transformed row with the correct structure for core table
    transformed_row = {
        "Video_ID": row["Video_ID"],
        "Video_Title": row["Video_Title"],
        "Upload_Date": row["Upload_Date"],
        "Duration": int(duration_td.total_seconds()),  # Convert to integer seconds
        "Video_Type": "Shorts" if duration_td.total_seconds() <= 60 else "Normal",
        "Video_Views": row["Video_Views"],
        "Likes_Count": row["Likes_Count"],
        "Comments_Count": row["Comments_Count"]
    }
    
    return transformed_row