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
        for member in channel_members[0:5]:
            print(member)
            fids.add(member.get('fid'))
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
        
    def get_user_casts_in_channel(self, channel, fids):
        endpoint = "feed/user/casts"
        all_filtered_casts = []
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=int(self.manual_cutoff))
        headers = {
            'accept': 'application/json',
            'api_key': self.NEYNAR_API_KEY
        }

        for fid in fids:
            self.logger.info(f"Collecting casts for user {fid} in channel {channel}.....")
            params = {
                'fid': fid, 
                'api_key': self.NEYNAR_API_KEY,
                'limit': 150,
                'channel_fid': channel
            }
            try:
                casts_in_channel = helpers.query_neynar_api(endpoint, params, headers)
                for cast in casts_in_channel:
                    if 'timestamp' in cast:
                        try:
                            cast_timestamp = datetime.strptime(cast['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
                        except ValueError:
                            # If the above fails, try parsing without milliseconds
                            cast_timestamp = datetime.strptime(cast['timestamp'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                        print(f"Cast timestamp: {cast_timestamp}")
                        if cast_timestamp >= cutoff_date:
                            all_filtered_casts.append(cast)
            except Exception as e:
                self.logger.error(f"Error fetching casts in channel {channel} for user {fid}: {e}")
                continue

        return all_filtered_casts
        
    def get_all_user_channels(self, fids):
        for fid in fids:
            self.logger.info(f"Collecting additional channels for user {fid}...")
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
                self.logger.error(f"Error fetching additional channels for user {fid}")
                return None 
            


        
    def run(self):
        for channel in self.channels:
            channel_dict = self.channel_dict = {}
            channel_dict['channel_metadata'] = self.get_channel_metadata(channel)
            members = self.get_channel_members(channel)
            followers = self.get_channel_followers(channel)
            all_channel_fids = self.get_all_channel_fids(members, followers)
            channel_dict['members'] = members 
            channel_dict['followers'] = followers 
            channel_dict['all_channel_fids'] = all_channel_fids
            channel_dict['casts'] = self.get_user_casts_in_channel(channel, all_channel_fids)
            channel_dict['all_channels'] = self.get_all_user_channels(all_channel_fids)
            self.data['channels_data'] = channel_dict
            helpers.save_data(
                self.s3_client,
                self.BUCKET_NAME,
                "data_" + "farcaster_" + str(self.runtime) + '.json',
                self.data
            )


if __name__ == "__main__":
    fethcer = FetchFarcasterHubData()
    fethcer.run()
