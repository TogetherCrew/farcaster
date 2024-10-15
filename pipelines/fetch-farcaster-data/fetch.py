import os 
import time 
import json 
import boto3 
import logging
from . import helpers


import requests as r
from requests import RequestException

from datetime import datetime, timezone, timedelta
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv

load_dotenv()

class FetchFarcasterHubData:
    def __init__(self):
        self.NEYNAR_API_KEY = os.getenv("NEYNAR_API_KEY")
        self.FARCASTER_EPOCH = datetime(2021, 1, 1, tzinfo=timezone.utc)
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.BUCKET_NAME = os.getenv('BUCKET_NAME')
        self.data = {}
        
        if not self.AWS_ACCESS_KEY_ID or not self.AWS_SECRET_ACCESS_KEY:
            raise ValueError("AWS credentials are missing. Please check your environment variables.")

        self.s3_client = boto3.client(
            's3', 
            region_name='us-east-1',
            aws_access_key_id=self.AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY
        )
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.metadata = helpers.load_metadata(self.s3_client, self.BUCKET_NAME, self.logger)
        self.last_run = self.metadata.get('last_run', None)

        self.channels = ['optimism']

    
    def get_channel_metadata(self, channel_name):
        headers = {
            'accept': 'application/json',
            'api_key': self.NEYNAR_API_KEY
        }
        params = {
            'id': channel_name
        }
        endpoint = 'channel'
        try:
            channel_metadata = helpers.query_neynar_api(endpoint, params, headers) 
            return channel_metadata
        except r.RequestException as e:
            self.logger.error(f"Error fetching channel metadata: {e}")
            return None
        
    def get_channel_members(self, channel_id):
        headers = {
            'accept': 'application/json',
            'api_key': self.NEYNAR_API_KEY
        }
        endpoint = 'channel/member/list'
        params = {
            'channel_id': channel_id,
            'limit': 100
        }
        try:
            channel_members = helpers.query_neynar_api(endpoint, params, headers) 
            return channel_members
        except r.RequestException as e:
            self.logger.error(f"Error fetching channel metadata: {e}")
            return None

    def get_channel_followers(self, channel_id):
        headers = {
            'accept': 'application/json',
            'api_key': self.NEYNAR_API_KEY
        }
        endpoint = 'channel/followers'
        params = {
            'id': channel_id,
            'limit': 1000
        }
        try:
            channel_followers = helpers.query_neynar_api(endpoint, params, headers)
            return channel_followers
        except r.RequestException as e:
            self.logger.error(f"Error fetching channel followers: {e}")

        
    def get_channel_casts(self, channel_id):
        endpoint = 'feed/channels'
        headers = {
            'accept': 'application/json',
            'api_key': self.NEYNAR_API_KEY
        }
        params = {
            'channel_ids': channel_id,
            'with_replies': False, 
            'limit': 100
        }
        try:
            channel_casts = helpers.query_neynar_api(endpoint, params, headers)
            return channel_casts 
        except r.RequestException as e:
            self.logger.error(f"Error fetching channel casts: {e}")
            return None 


#### def get_following()
    def get_user_casts(self, fid):
        self.logger.info(f"Collecting casts for user {fid}.....")
        endpoint = "castsByFid"
        params = {'fid': fid, 'api_key': self.NEYNAR_API_KEY}
        try:
            messages = helpers.query_neynar_hub(endpoint=endpoint, params=params)
            return messages
        except Exception as e:
            self.logger.error(f"Error fetching casts for user: {e}")
            return None



    def get_user_follows(self, fid):
        endpoint = "linksByFid"
        params = {
            'fid': str(fid), 
            'link_type': 'follow'
        }
        messages = self.query_neynar_hub(endpoint=endpoint, params=params)

        return [{
            'source': str(fid),
            'target': str(item['data']['linkBody'].get('targetFid')),
            'timestamp': item['data'].get('timestamp'),
            'edge_type': 'FOLLOWS'
        } for item in messages 
          if "data" in item 
          and "linkBody" in item["data"] 
          and item['data']['linkBody'].get('targetFid') 
          and item['data'].get('timestamp')]

    def get_user_likes(self, fid):
        endpoint = "reactionsByFid"
        params = {
            'fid': fid,
            'reaction_type': 'REACTION_TYPE_LIKE'
        }
        messages = self.query_neynar_hub(endpoint, params)

        return [{
            'source': str(fid),
            'target': str(item['data']['reactionBody']['targetCastId'].get('fid')),
            'target_hash': item['data']['reactionBody']['targetCastId'].get('hash'),
            'timestamp': item['data'].get('timestamp'),
            'edge_type': 'LIKED'
        } for item in messages 
          if "data" in item 
          and "reactionBody" in item["data"] 
          and item['data']['reactionBody'].get('targetCastId') 
          and item['data'].get('timestamp')]

    def get_user_recasts(self, fid):
        endpoint = "reactionsByFid"
        params = {
            'fid': fid,
            'reaction_type': 'REACTION_TYPE_RECAST'
        }
        messages = self.query_neynar_hub(endpoint, params)

        return [{
            'source': str(fid),
            'target': str(item['data']['reactionBody']['targetCastId'].get('fid')),
            'target_hash': item['data']['reactionBody']['targetCastId'].get('hash'),
            'timestamp': item['data'].get('timestamp'),
            'edge_type': 'RECASTED'
        } for item in messages 
          if "data" in item 
          and "reactionBody" in item["data"] 
          and item['data']['reactionBody'].get('targetCastId') 
          and item['data'].get('timestamp')]

    # def get_user_casts(self, fid):
    #     self.logger.info(f"Collecting casts for user {fid}.....")
    #     endpoint = "castsByFid"
    #     params = {'fid': fid}
    #     messages = self.query_neynar_hub(endpoint=endpoint, params=params)

    #     cast_data_list = [{
    #         'source': str(fid),
    #         'target': str(message['data']['castAddBody']['parentCastId']['fid']),
    #         'timestamp': message['data']['timestamp'],
    #         'edge_type': 'REPLIED'
    #     } for message in messages 
    #       if 'data' in message 
    #       and 'castAddBody' in message['data'] 
    #       and message['data']['castAddBody'].get('parentCastId')]

    #     self.logger.info(f"Retrieved {len(cast_data_list)} replies for user: {fid}...")
    #     return cast_data_list

    # def get_user_data(self, fid):
    #     return {
    #         'core_node_metadata': self.get_user_metadata(fid),
    #         'likes': self.get_user_likes(fid),
    #         'recasts': self.get_user_recasts(fid),
    #         'casts': self.get_user_casts(fid),
    #         'following': self.get_user_follows(fid)
    #     }

    def collect_connections_ids(self, user_object):
        unique_fids = set()
        
        if 'core_node_metadata' in user_object and 'fid' in user_object['core_node_metadata']:
            unique_fids.add(user_object['core_node_metadata']['fid'])
        
        for key in ['following', 'likes', 'recasts', 'casts']:
            if key in user_object:
                unique_fids.update(item['target'] for item in user_object[key] if 'target' in item)
                unique_fids.update(item['source'] for item in user_object[key] if 'source' in item)

        return unique_fids


    def run(self):
        # channel_data = self.get_channel_metadata('optimism')
        # channel_members = self.get_channel_members('optimism')
        # channel_followers = self.get_channel_followers('optimism')
        # channel_casts = self.get_channel_casts('optimism')
        casts_test = self.get_user_casts('190000')
        recasts_test = 
        likes_test = 
        channel_memberships = 
        following = 
        print(casts_test)

if __name__ == "__main__":
    fethcer = FetchFarcasterHubData()
    fethcer.run()


    

        

