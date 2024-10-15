from datetime import datetime, timezone, timedelta
import json
import logging
import time
import requests as r
from requests import RequestException
from botocore.exceptions import ClientError



# def convert_timestamp(timestamp, epoch):
#     """Converts Farcaster Hub Timestamp to UTC datetime"""
#     return epoch + timedelta(seconds=int(timestamp))

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
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info(f"Bucket {bucket_name} does not exist. Creating...")
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"Bucket {bucket_name} created successfully.")
            except ClientError as create_error:
                logger.error(f"Failed to create bucket: {create_error}")
                return
        else:
            logger.error(f"Error checking bucket: {e}")
            return
    save_data(s3_client, bucket_name, 'metadata.json', metadata, logger)

def load_metadata(s3_client, bucket_name, logger):
    """Loads metadata from S3"""
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key='metadata.json')
        metadata = json.loads(obj['Body'].read().decode('utf-8'))
        logger.info("Metgadata loaded.")
        return metadata
    except s3_client.exceptions.NoSuchKey:
        logger.info("No metadata found. Starting fresh.")
        return {}
    except ClientError as e:
        logger.error(f"Failed to load metadata: {e}")
        return {}

def query_neynar_hub(endpoint, params=None):
    base_url = "https://hub-api.neynar.com/v1/"
    headers = {
        "Content-Type": "application/json"
    }
    url = f"{base_url}{endpoint}"
    params = params or {}
    params['pageSize'] = 1000

    all_messages = []
    max_retries = 3
    retry_delay = 1

    while True:
        for attempt in range(max_retries):
            try:
                time.sleep(0.1)
                response = r.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                if 'messages' in data:
                    for message in data['messages']:
                        if 'timestamp' in message.get('data', {}):
                            all_messages.extend(data['messages'])
                            print(f"Retrieved {len(all_messages)} messages total...")
                
                if 'nextPageToken' in data and data['nextPageToken']:
                    params['pageToken'] = data['nextPageToken']
                    break
                else:
                    return all_messages
                
            except RequestException as e:
                if attempt == max_retries - 1:
                    print(f"Failed after {max_retries} attempts. Error: {e}")
                    return all_messages
                else:
                    print(f"Attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2

def query_neynar_api(endpoint, params, headers):
    base_url = "https://api.neynar.com/v2/farcaster/"
    url = f"{base_url}{endpoint}"
    all_items = []

    counter = 0
    
    try:
        while True:
            print(f"Collecting data, on iteration {counter}...")
            response = r.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Get the first key in the data dictionary
            first_key = next(iter(data))
            # Extend all_items with the list under the first key
            all_items.extend(data[first_key])
            
            if 'next' in data and 'cursor' in data['next']:
                params['cursor'] = data['next']['cursor']
                counter += 1
            else:
                break
    except r.RequestException as e:
        print(f"Request failed: {e}")
        if hasattr(e.response, 'status_code') and e.response.status_code == 400:
            print(f"Bad request. Response content: {e.response.text}")
    
    return all_items
