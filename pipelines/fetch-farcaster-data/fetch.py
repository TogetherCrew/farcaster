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
        print(f"Fetching members for channel_id: {channel_id}")
        try:
            channel_members = helpers.query_neynar_api(endpoint, params, headers)
            return channel_members
        except r.RequestException as e:
            self.logger.error(f"Error fetching channel members: {e}")
            return None
        
    def get_channel_followers(self, channel_id):
        print(f"Fetching followers for channel {channel_id}...")
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


    def get_channel_members(self, channel_id):
        if not isinstance(channel_id, (str, int)):
            self.logger.error(f"Invalid channel_id type: {type(channel_id)}. Expected str or int.")
            return None

        headers = {
            'accept': 'application/json',
            'api_key': self.NEYNAR_API_KEY
        }

        endpoint = 'channel/member/list'

        params = {
            'channel_id': channel_id,
            'limit': 100
        }

        print(f"Fetching members for channel_id: {channel_id}")
        try:
            channel_members = helpers.query_neynar_api(endpoint, params, headers)
            return channel_members
        except r.RequestException as e:
            self.logger.error(f"Error fetching channel members: {e}")
            return None


    def get_all_fids_channel_members(self, channel_members):
        fids = set()
        for member in channel_members:
            fids.add(member.get('user').get('fid'))
        return list(fids)
        

    def get_all_channel_fids(self, followers, members):
        members_ids = self.get_all_fids_channel_members(members)
        followers_ids = self.get_all_fids_channel_followers(followers)
        return list(set(members_ids + followers_ids))



    
    def get_all_fids_channel_followers(self, channel_followers):
        fids = set()
        for follower in channel_followers:
            fids.add(follower.get('fid'))
        return list(fids)
    
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
        
        # Get channel followers

    def get_user_casts_in_channel(self, fid, channel):
        self.logger.info(f"Collecting casts for user {fid} in channel {channel}.....")
        endpoint = "feed/user/casts"
        params = {'fid': fid, 'api_key': self.NEYNAR_API_KEY, 'limit': 150,
                  'channel_fid': channel}
        headers = {
            'accept': 'application/json',
            'api_key': self.NEYNAR_API_KEY
        }
        try:
            casts_in_channel = helpers.query_neynar_api(endpoint, params, headers)
            return casts_in_channel
        except Exception as e:
            self.logger.error(f"Error fetching casts in channel {channel} for user: {e}")
            return None
        


        
    def run(self):
        # followers = self.get_channel_followers('optimism')
        # members = self.get_all_fids_channel_members('optimism')
        # all_channel_fids = self.get_all_channel_fids(followers, members)
        channel_member_casts = self.get_user_casts_in_channel('195960', 'optimism')
        print(channel_member_casts[0:5])

        


# new strategy...
# - capture timestamp and convert it
# - check metadata
# - get all casts, recasts, etc for all users as starting point (for profiling)
#     - Param for up to, if null, all 
#         - change the run function to accomodate this logic 
# - If no metadata default to config, if metadata, adjust timestamp to config



if __name__ == "__main__":
    fethcer = FetchFarcasterHubData()
    fethcer.run()
