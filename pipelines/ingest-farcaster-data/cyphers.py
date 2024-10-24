from tqdm import tqdm
from .helpers import get_query_logging, count_query_logging, Cypher


class FarcasterCyphers(Cypher):
    def __init__(self):
        super().__init__()



    @count_query_logging
    def create_or_merge_channels(self, urls):
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
    
    @count_query_logging
    def connect_channel_members(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (member:User:Farcaster {{fid: 'rows.fid'}}) 
            MATCH (channel:Channel)
            MERGE (member)-[r:MEMBER]->(channel)
            RETURN COUNT(member)
            """
            count += self.query(query)[0]
        return count 
    
    
    @count_query_logging
    def connect_channel_moderators(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            WITH rows
            MATCH (channel:Channel {{id: 'rows.id'}})
            MATCH (user:User:Farcaster {{fid: 'rows.fid'}})
            MERGE (user)-[r:MODERATOR]->(channel)
            RETURN COUNT(r)
            """
            count += self.query(query)[0]
        return count 

    @count_query_logging
    def create_followers_set_properties(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (user:User:Farcaster {{fid: 'rows.fid'}})
            SET user.username = rows.username
            SET user.displayName = rows.display_name
            SET user.powerBadge = rows.power_padge
            RETURN COUNT(user)
            """
            count += self.query(query)[0]
        return count 
    

    @count_query_logging
    def create_connect_custody_wallets(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MATCH (user:User:Farcaster {{fid: 'rows.fid'}})
            MERGE (wallet:Wallet:Farcaster {{address: rows.custody_address}})
            MERGE (user)-[r:ACCOUNT]->(wallet)
            SET r.source = 'Farcaster'
            SET r.type = 'custody_address'
            RETURN COUNT(r) 
            """
            count += self.query(query)[0]
        return count 
    

    
