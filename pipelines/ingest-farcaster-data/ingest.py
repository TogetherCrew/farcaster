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
        channels_urls = self.save_df_as_csv(channels_df_filtered, f'channels_data_{self.asOf}.csv')
        print(channels_urls)
        self.cyphers.create_or_merge_channels(channels_urls)
        

    def connect_channel_leads(self):

        return None 
    

    def create_users(self):
        data = self.data

    def set_user_metadata(self):

        return None 
    
    def connect_wallets(self):

        return None
    
    def connect_other_channels(self):

        return None 

    def connect_channel_leads(self):

        return None 

    def connect_channel_members(self):

        return None 

    def connect_channel_followers(self):

        return None  
    
    def create_channel_casts(self):

        return None 
    
    def connect_channel_casts(self):

        return None 

    def run(self):
        self.create_or_merge_channels()
        pass

if __name__ == "__main__":
    ingester = FarcasterIngester()
    ingester.run()