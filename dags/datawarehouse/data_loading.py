#funtion for open the file json , read the file and parse it inot python object 

import json
from datetime import date
import logging
import os

logger = logging.getLogger(__name__)

def load_data():
    # First, let's check what files actually exist in the data directory
    data_dir = "./data"
    
    # Check if data directory exists
    if not os.path.exists(data_dir):
        logger.error(f"Data directory '{data_dir}' does not exist")
        raise FileNotFoundError(f"Data directory '{data_dir}' does not exist")
    
    # List all files in the data directory
    files = os.listdir(data_dir)
    logger.info(f"Files in data directory: {files}")
    
    # Try to find a YT_data or YI_data file
    target_files = [f for f in files if f.startswith(('YT_data_', 'YI_data_')) and f.endswith('.json')]
    
    if not target_files:
        logger.error(f"No YT_data or YI_data JSON files found in {data_dir}")
        raise FileNotFoundError(f"No YT_data or YI_data JSON files found in {data_dir}")
    
    # Use the most recent file
    file_to_load = sorted(target_files)[-1]
    file_path = os.path.join(data_dir, file_to_load)
    
    try:
        logger.info(f"Processing file: {file_path}")
        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)
        logger.info(f"Successfully loaded {len(data)} records")
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in file: {file_path} - {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading data from {file_path}: {e}")
        raise