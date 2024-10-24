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
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.BUCKET_NAME = os.getenv('BUCKET_NAME')
        self.data = {}
        self.runtime = datetime.now().strftime("%Y-%m-%d-%H-%M")
        self.cutoff = os.getenv('CUTOFF')
        self.channels = json.loads(os.getenv('CHANNEL_IDS', '[]'))
        
        self.s3_client = boto3.client(
            's3', 
            region_name='us-east-1',
            aws_access_key_id=self.AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY
        )

        logging.basicConfig(level=logging.INFO)
        self.data = {
                    'members': [],
                    'followers': [],
                    'all_channel_fids': [],
                    'all_channel_casts': [],
                    'followed_channels': []
                }
                
                
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
            logging.error(f"Error fetching channel followers: {e}")


    def get_channel_members(self, channel_id):
        if not isinstance(channel_id, (str, int)):
            logging.error(f"Invalid channel_id type: {type(channel_id)}. Expected str or int.")
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
            logging.error(f"Error fetching channel members: {e}")
            return None


    def get_all_fids_channel_members(self, channel_members):
        fids = set()
        for member in channel_members[0:5]:
            print(member)
            fids.add(member.get('fid'))
        return list(fids)
        
    
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
            logging.error(f"Error fetching channel metadata: {e}")
            return None
        
    def get_channel_casts(self, channel):
        logging.info(f"Getting casts for channel: {channel}")
        endpoint = "feed/channels"
        all_filtered_casts = []
        headers = {
            'accept': 'application/json',
            'api_key': self.NEYNAR_API_KEY
        }
        params = {
            'channel_ids': channel, 
            'limit': 100,
            'with_replies': True
        }
        all_filtered_casts = helpers.query_neynar_api(endpoint, params, headers, cutoff_days=self.cutoff)
        return all_filtered_casts
        
    def get_all_user_channels(self, fids):
        for fid in fids:
            logging.info(f"Collecting additional channels for user {fid}...")
            endpoint = "user/channels"
            headers = {
                'accept': 'application/json',
                'api_key': self.NEYNAR_API_KEY
            }
            params = {
                'fid': fid, 
                'limit': 100,
            }
            try:
                additional_channels = helpers.query_neynar_api(endpoint, params, headers)
                return additional_channels
            except Exception as e:
                logging.error(f"Error fetching additional channels for user {fid}")
                return None 
                    

    ### return dict with "channels" that is a list of these I can iterate through
    def run(self):
        for channel in self.channels:
            channel_dict = self.channel_dict = {}
            channel_dict["channel"] = channel
            channel_dict['channel_metadata'] = self.get_channel_metadata(channel)
            members = self.get_channel_members(channel)
            followers = self.get_channel_followers(channel)
            channel_dict['members'] = members 
            channel_dict['followers'] = followers 
            # Extract fids from members and followers
            member_fids = [str(member['user']['fid']) for member in members if 'user' in member and 'fid' in member['user']]
            follower_fids = [str(follower['fid']) for follower in followers if 'fid' in follower]
            all_fids = list(set(member_fids + follower_fids))
            channel_dict['all_channels'] = self.get_all_user_channels(followers)
            channel_dict['all_followed_channels'] = self.get_all_user_channels(all_fids)
            
            helpers.save_data(
                self.s3_client,
                self.BUCKET_NAME,
                "data_" + "farcaster_" + str(self.runtime) + '.json',
                self.data
            )
            channel_casts = self.get_channel_casts(channel)
            print(len(channel_casts))


if __name__ == "__main__":
    fethcer = FetchFarcasterHubData()
    fethcer.run()
