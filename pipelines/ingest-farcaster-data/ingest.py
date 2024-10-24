from .helpers import Ingestor, Cypher

from .cyphers import FarcasterCyphers
from datetime import datetime

import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

class FarcasterIngester(Ingestor):
    def __init__(self):
        self.cyphers = FarcasterCyphers() 
        self.asOf = datetime.now().timestamp()
        super().__init__("tc-farcaster-data")  # Bucket name here



    def create_or_merge_channels(self):
        channels_data = self.scraper_data['channels_data']['all_channels']
        channels_df = pd.DataFrame(channels_data)
        channels_df_filtered = channels_df[['id', 'url', 'name', 'description']]
        channels_df_filtered.dropna(inplace=True)
        channels_urls = self.save_df_as_csv(channels_df_filtered, f'channels_metadata_{self.asOf}.csv')
        self.cyphers.create_or_merge_channels(channels_urls)


    def create_channel_followers(self):
        """
        Creates followers & non-nested metadata
        """
        all_channel_followers = self.scraper_data['channels_data']['followers']
        followers_df = pd.DataFrame([{
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
    # def connect_socials_to_followers(self):
    
    def connect_channel_members(self):
        all_channel_members = self.scraper_data['channels_data']['members']
        all_channel_members_df = pd.DataFrame([{'fid': user['user']['fid'], 'channel': user['channel']['id']} for user in all_channel_members])
        channel_members_urls = self.save_df_as_csv(all_channel_members_df, f'channel_members_{self.asOf}.csv')
        self.cyphers.connect_channel_members(channel_members_urls)


    def connect_channel_moderators(self):
        channels_data = self.scraper_data['channels_data']['all_channels']
        moderators_df = pd.DataFrame(channels_data)
        moderators_df_filtered = moderators_df[['id', 'moderator_fids']]
        moderators_df_filtered = moderators_df_filtered.explode('moderator_fids')
        moderators_df_filtered.dropna(inplace=True)
        moderators_urls = self.save_df_as_csv(moderators_df_filtered, f'moderators_{self.asOf}.csv')
        self.cyphers.connect_channel_moderators(moderators_urls)
        return None 
    


    def set_user_metadata(self):

        return None 
    
    def connect_wallets(self):

        return None
    
    def connect_other_channels(self):

        return None 


    def connect_channel_followers(self):

        return None  
    
    def create_channel_casts(self):

        return None 
    
    def connect_channel_casts(self):

        return None 

    def run(self):
        # self.create_channel_followers()
        # self.connect_channel_members()
        # self.create_or_merge_channels()
        self.connect_channel_moderators()
        pass

if __name__ == "__main__":
    ingester = FarcasterIngester()
    ingester.run()