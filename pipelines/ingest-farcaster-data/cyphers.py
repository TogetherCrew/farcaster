from tqdm import tqdm
from .helpers import get_query_logging, count_query_logging, Cypher


class FarcasterCyphers(Cypher):
    def __init__(self):
        super().__init__()



    @count_query_logging
    def create_or_merge_channels(self, urls):
        print(urls)
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            WITH rows
            MERGE (channel:Channel:Farcaster {{id: 'rows.id'}})
            ON CREATE SET
                channel.name = rows.name,
                channel.url = rows.url, 
                channel.description = rows.description
            RETURN COUNT(channel)
            """
            count += self.query(query)[0]
        return count 
