from datetime import timedelta,datetime

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
    row["Duration_Seconds"] = duration_td.total_seconds()
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"
    return row