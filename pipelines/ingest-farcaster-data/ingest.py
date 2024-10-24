from .helpers import Ingestor, Cypher

from .cyphers import FarcasterCyphers
from datetime import datetime
import os 
from dotenv import load_dotenv
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
load_dotenv()

class FarcasterIngester(Ingestor):
    def __init__(self):
        self.cyphers = FarcasterCyphers() 
        self.asOf = datetime.now().timestamp()
        self.channels = os.getenv('CHANNEL_IDS')
        super().__init__("tc-farcaster-data")  # Bucket name here


    def create_indexes(self):
        self.cyphers.create_user_fid_index()
        self.cyphers.create_cast_index()
        self.cyphers.create_wallet_index()
        self.cyphers.create_channel_id_index()


    def create_or_merge_channels(self):
        channels_data = self.scraper_data['channels_data']['all_channels']
        channels_df = pd.DataFrame(channels_data)
        channels_df_filtered = channels_df[['id', 'url', 'name', 'description']]
        channels_df_filtered.dropna(inplace=True)
        channels_urls = self.save_df_as_csv(channels_df_filtered, f'channels_metadata_{self.asOf}.csv')
        self.cyphers.create_or_merge_channels(channels_urls)


    def create_channel_followers(self, channel):
        """
        Creates followers & non-nested metadata
        """
        print(self.scraper_data.keys())
        print(self.scraper_data['followers'][0:1])
        all_channel_followers = self.scraper_data['followers']
        followers_df = pd.DataFrame([{
            'channelId': channel,
            'fid': str(user.get('fid')),
            'username': self.cyphers.sanitize_text(user.get('username', '')),
            'display_name': self.cyphers.sanitize_text(user.get('display_name', '')),
            'custody_address': user.get('custody_address', ''),
            'bio_text': self.cyphers.sanitize_text(user.get('profile', {}).get('bio', {}).get('text', '')),
            'verifications': ','.join(user.get('verifications', [])),
            'verified_accounts': str(user.get('verified_accounts', '')),
            'power_badge': user.get('power_badge', '')
        } for user in all_channel_followers if isinstance(user, dict)])        
        followers_and_properties_df = followers_df[['fid', 'username', 'custody_address', 'bio_text', 'power_badge']]
        followers_and_properties_urls = self.save_df_as_csv(followers_and_properties_df, f'channel_followers_{self.asOf}.csv')
        self.cyphers.create_followers_set_properties(followers_and_properties_urls)
        """
        Connects followers to custody wallets
        """
        followers_and_custody_wallets_df = followers_df[['fid', 'custody_address']]
        follwers_and_custody_wallets_urls = self.save_df_as_csv(followers_and_custody_wallets_df, f"followers_custody_wallets_{self.asOf}.csv")
        self.cyphers.create_connect_custody_wallets(follwers_and_custody_wallets_urls)
        """
        Connect followers to other followed channels
        """
        additional_channels = self.scraper_data['channels_data']['all_followed_channels']
        additional_channels_df = additional_channels[['id', 'fid']]
        additional_channels_df['fid'] = additional_channels_df['fid'].astype(str)
        additional_channels_urls = self.save_df_as_csv(additional_channels_df, f"all_channel_members_{self.asOf}.csv")
        self.cyphers.connect_additional_channel_memberships(additional_channels_urls)


    
    def connect_channel_members(self, channel):
        all_channel_members = self.scraper_data['channels_data']['members']
        all_channel_members_df = pd.DataFrame([{'fid': str(user['user']['fid'])} for user in all_channel_members])
        all_channel_members_df['channel'] = channel
        channel_members_urls = self.save_df_as_csv(all_channel_members_df, f'channel_members_{self.asOf}.csv')
        self.cyphers.connect_channel_members(channel_members_urls)


    def connect_channel_moderators(self):
        channels_data = self.scraper_data['channels_data']['all_channels']
        moderators_df = pd.DataFrame(channels_data)
        moderators_df_filtered = moderators_df[['id', 'moderator_fids']]
        moderators_df_filtered = moderators_df_filtered.explode('moderator_fids')
        moderators_df_filtered = moderators_df_filtered.rename(columns={'moderator_fids': 'fid'})
        moderators_df_filtered['fid'] = moderators_df_filtered['fid'].astype(str)
        moderators_df_filtered.dropna(inplace=True)
        moderators_urls = self.save_df_as_csv(moderators_df_filtered, f'moderators_{self.asOf}.csv')
        self.cyphers.connect_channel_moderators(moderators_urls)
        return None 
    

    def create_connect_channel_casts(self):
        casts_data = self.scraper_data['channels_data']['casts']
        """
        Create casts and properties
        """
        casts_df = pd.DataFrame([{
            'hash': cast['hash'],
            'thread_hash': cast['thread_hash'],
            'parent_hash': cast['parent_hash'],
            'author_fid': str(cast['author']['fid']),
            'text': self.cyphers.sanitize_text(cast['text']),
            'timestamp': cast['timestamp'],
            'replies_count': cast['replies']['count'],
            'recasts_count': cast['reactions']['recasts_count'],
            'likes_count': cast['reactions']['likes_count']
        } for cast in casts_data])
        casts_urls = self.save_df_as_csv(casts_df, f"casts_{self.asOf}.csv")
        self.cyphers.create_casts(casts_urls)
        """
        Connect authors
        """
        self.cyphers.connect_casts_authors()
        """
        Connect parent cast
        """
        self.cyphers.connect_casts_parent_cast()

        """
        Connect Likes
        """
        likes_df = pd.DataFrame([
            {'hash': cast['hash'], 'fid': str(like['fid'])} 
            for cast in casts_data 
            for like in cast['reactions']['likes']
        ])
        likes_urls = self.save_df_as_csv(likes_df, f"cast_likes_{self.asOf}.csv")
        self.cyphers.connect_cast_likes(likes_urls)
        """
        Connect recasts
        """
        recasts_df = pd.DataFrame([
        {'hash': cast['hash'], 'fid': str(recast['fid'])} 
        for cast in casts_data 
        for recast in cast['reactions']['recasts']
        ])
        recasts_urls = self.save_df_as_csv(recasts_df, f"cast_recasts_{self.asOf}.csv")
        self.cyphers.connect_cast_recasts(recasts_urls)

    def run(self):
        # self.create_indexes()
        for channel in self.channels:
            self.create_channel_followers(channel)
            # self.connect_channel_members(channel)
            # self.create_or_merge_channels()
            # self.connect_channel_moderators()
            # self.create_connect_channel_casts()

if __name__ == "__main__":
    ingester = FarcasterIngester()
    ingester.run()