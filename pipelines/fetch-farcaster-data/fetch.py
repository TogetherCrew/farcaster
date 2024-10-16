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
        self.manual_cutoff = 7 
        
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


    def convert_manual_cutoff_timestamp(self, manual_cutoff=None):
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.manual_cutoff)
        return int((cutoff_date - self.FARCASTER_EPOCH).total_seconds())

    def get_all_fids_channel_casts(self, casts_data):
        fids = set()
        for item in casts_data:
            if 'data' in item:
                if 'fid' in item['data']:
                    fids.add(item['data']['fid'])
                if 'parent' in item['data'] and 'fid' in item['data']['parent']:
                    fids.add(item['data']['parent']['fid'])
        return list(fids)
    
    def get_all_fids_channel_members(self, channel_members):
        fids = set()
        for member in channel_members:
            if 'user' in member and 'fid' in member['user']:
                fids.add(member['user']['fid'])
        return list(fids)
    
    def get_all_fids_channel_followers(self, followers_data):
        fids = set()
        for follower in followers_data:
            if 'user' in follower and 'fid' in follower['user']:
                fids.add(follower['user']['fid'])
        return list(fids)
    

    def collect_all_user_fids(self, channel_members, channel_followers, channel_casters):
        channel_member_fids = self.get_all_fids_channel_members(channel_members)
        channel_follower_fids = self.get_all_fids_channel_followers(channel_followers)
        channel_casts_fids = self.get_all_fids_channel_casts(channel_casters)
        return list(set(channel_member_fids + channel_follower_fids + channel_casts_fids))
        
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
        

new strategy...
- capture timestamp and convert it
- check metadata
- get all casts, recasts, etc for all users as starting point (for profiling)
    - Param for up to, if null, all 
        - change the run function to accomodate this logic 
- If no metadata default to config, if metadata, adjust timestamp to config


    def run(self):

if __name__ == "__main__":
    fethcer = FetchFarcasterHubData()
    fethcer.run()
