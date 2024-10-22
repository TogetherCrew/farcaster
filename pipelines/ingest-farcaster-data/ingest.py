from .helpers import Ingestor, Cypher
from .cyphers import FarcasterCyphers
import logging

logging.basicConfig(level=logging.INFO)

class FarcasterIngester(Ingestor):
    def __init__(self):
        self.cyphers = FarcasterCyphers()  # Using base Cypher class directly
        super().__init__("tc-farcaster-data")  # Your bucket name here
        self.data = self.scraper_data
    
    def gay(self):
        data = self.data
        print(data.keys())

    def run(self):
        self.gay()
        pass

if __name__ == "__main__":
    ingester = FarcasterIngester()
    ingester.run()