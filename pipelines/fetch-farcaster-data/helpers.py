from datetime import datetime, timezone, timedelta
import json
import time
import requests as r
from requests import RequestException
from botocore.exceptions import ClientError

def convert_timestamp(timestamp, epoch):
    """Converts Farcaster Hub Timestamp to UTC datetime"""
    return epoch + timedelta(seconds=int(timestamp))

def save_data(s3_client, bucket_name, key, data, logger):
    """Saves data to S3"""
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))
        logger.info(f"Data saved to {key}")
    except ClientError as e:
        logger.error(f"Failed to save data: {e}")

def save_metadata(s3_client, bucket_name, metadata, logger):
    """Saves metadata to S3"""
    metadata['last_run'] = datetime.utcnow().isoformat()
    save_data(s3_client, bucket_name, 'metadata.json', metadata, logger)

def load_metadata(s3_client, bucket_name, logger):
    """Loads metadata from S3"""
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key='metadata.json')
        metadata = json.loads(obj['Body'].read().decode('utf-8'))
        logger.info("Metadata loaded.")
        return metadata
    except s3_client.exceptions.NoSuchKey:
        logger.info("No metadata found. Starting fresh.")
        return {}
    except ClientError as e:
        logger.error(f"Failed to load metadata: {e}")
        return {}

def query_neynar_hub(base_url, endpoint, headers, params, convert_timestamp_func, epoch, logger):
    """Queries the Neynar Hub API and retrieves all messages"""
    url = f"{base_url}{endpoint}"
    params = params or {}
    params["page_size"] = 1000
    
    all_messages = []
    max_retries = 3
    retry_delay = 1 

    while True:
        for attempt in range(max_retries):
            try:
                time.sleep(0.1)
                response = r.get(url, headers=headers, params=params)
                response.raise_for_status()
                hub_data = response.json()

                if "messages" in hub_data:
                    for message in hub_data["messages"]:
                        if 'timestamp' in message.get('data', {}):
                            message['data']['timestamp'] = convert_timestamp_func(
                                message['data']['timestamp'], epoch
                            ).isoformat()
                    all_messages.extend(hub_data['messages'])
                    logger.info(f"Retrieved {len(all_messages)} messages total...")
                
                if 'nextPageToken' in hub_data and hub_data['nextPageToken']:
                    params['pageToken'] = hub_data['nextPageToken']
                    break
                else:
                    return all_messages
            except RequestException as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed after {max_retries} attempts. Error: {e}")
                    return all_messages
                else:
                    logger.warning(f"Attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2


def query_neynar_api(endpoint, params=None, headers=None, timeout=10, sleep_time=0.1):
    base_url = "https://api.neynar.com/v2/farcaster"
    if params is None:
        params = {}
    all_data = []
    while True:
        url = f"{base_url}{endpoint}"
        try:
            response = r.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data.get('members', []))
            cursor = data.get('nextPageToken')
            if not cursor:
                break
            params['cursor'] = cursor
            time.sleep(sleep_time)
        except r.RequestException:
            break
    return all_data





