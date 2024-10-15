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



    ### get_channel_members
        ### metadata
        ### following
        ### casts (ideally casts in channel?)
        ### recasts
        ### likes

    ### get_user_following

    ### get

    ### 
    ### add filters
    #### def get_all_data()

    ### iterate
    ### channel data
    ### users data
        ## casts
        ## mutual followers/following

        
    def run(self):
        # channel_data = self.get_channel_metadata('optimism')
        # channel_members = self.get_channel_members('optimism')
        channel_casts = self.get_channel_casts('optimism')
        print(len(channel_casts))

if __name__ == "__main__":
    fethcer = FetchFarcasterHubData()
    fethcer.run()


    

        

