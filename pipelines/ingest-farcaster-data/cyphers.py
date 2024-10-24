from tqdm import tqdm
from .helpers import get_query_logging, count_query_logging, Cypher
import logging 

class FarcasterCyphers(Cypher):
    def __init__(self):
        super().__init__()

    def create_user_fid_index(self):
        logging.info("Creating users index...")
        query = """CREATE INDEX farcaster_user_fid_index IF NOT EXISTS FOR (u:User:Farcaster) ON (u.fid)"""
        self.query(query)

    def create_cast_index(self):
        logging.info("Creating casts index...")
        query = """CREATE INDEX farcaster_cast_hash_index IF NOT EXISTS FOR (c:Cast) ON (c.hash)"""
        self.query(query)

    def create_wallet_index(self):
        logging.info("Creating wallets index...")
        query = """CREATE INDEX farcaster_wallet_address_index IF NOT EXISTS FOR (w:Wallet:Farcaster) ON (w.address)"""
        self.query(query)

    def create_channel_id_index(self):
        logging.info("Creating channels index...")
        query = """CREATE INDEX farcaster_channel_id_index IF NOT EXISTS FOR (c:Channel) ON (c.channelId)"""
        self.query(query)


    @count_query_logging
    def create_followers_set_properties(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (user:User:Farcaster {{fid: rows.fid}})
            SET user.username = rows.username
            SET user.bio = rows.bio_text
            SET user.displayName = rows.display_name
            SET user.powerBadge = rows.power_padge
            RETURN COUNT(user)
            """
            count += self.query(query)[0]
        return count 
    
    @count_query_logging
    def connect_followers_to_channels(self, urls, channel_id):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MATCH (user:User {{fid: rows.fid}})
            MATCH (channel:Channel {{channelId: '{channel_id}'}})
            MERGE (user)-[r:FOLLOW]->(channel)
            RETURN COUNT(user)
            """
            count += self.query(query)[0]
        return count 

    @count_query_logging
    def create_or_merge_channels(self, urls):
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (channel:Channel:Farcaster {{channelId: rows.id}})
            SET channel.name = rows.name
            SET channel.url = rows.url
            SET channel.description = rows.description
            SET channel.moderatorFids = rows.moderator_fids
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
            MERGE (member:User:Farcaster {{fid: rows.fid}}) 
            WITH member
            MATCH (channel:Channel {{channelId: rows.channelId}})
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
            MATCH (channel:Channel {{id: rows.id}})
            MATCH (user:User:Farcaster {{fid: rows.fid}})
            MERGE (user)-[r:MODERATOR]->(channel)
            RETURN COUNT(r)
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
            WITH user, wallet
            MERGE (user)-[r:ACCOUNT]->(wallet)
            SET r.source = 'Farcaster'
            SET r.type = 'custody_address'
            RETURN COUNT(r) 
            """
            count += self.query(query)[0]
        return count 
    
    @count_query_logging
    def create_casts(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (cast:Cast:Farcaster {{hash: rows.hash}})
            SET cast.authorFid = rows.author_fid
            SET cast.threadHash = rows.thread_hash
            SET cast.parentHash = rows.parent_hash
            SET cast.text = rows.text
            SET cast.repliesCount = 0 
            SET cast.recastsCount = 0
            SET cast.likesCount = 0 
            RETURN COUNT(cast)
            """
            count += self.query(query)[0]
        return count 
    
    @count_query_logging
    def connect_casts_authors(self):
        count = 0 
        query = """
        MATCH (cast:Cast:Farcaster)
        WHERE NOT (cast)-[:POSTED]-()
        WITH cast 
        MATCH (author:User:Farcaster)
        WHERE author.fid = cast.authorFid 
        MERGE (author)-[r:POSTED]->(cast)
        RETURN COUNT(author)
        """
        count += self.query(query)[0]
        return count 

    @count_query_logging
    def connect_casts_parent_cast(self):
        count = 0 
        query = """
        MATCH (cast:Cast)
        MATCH (parentCast:Cast)
        WHERE NOT (cast)-[]-(parentCast)
        AND cast.threadHash = parentCast.threadHash
        AND cast.parentHash = parentCast.hash 
        MERGE (cast)-[r:REPLIED]->(parentCast)
        RETURN COUNT(cast)
        """
        count += self.query(query)[0]
        return count 


    @count_query_logging
    def connect_cast_likes(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (user:User:Farcaster {{fid: rows.fid}})
            WITH user, rows.hash as hash
            MATCH (cast:Cast {{hash: hash}})
            WITH user, cast
            MERGE (user)-[r:LIKED]->(cast)
            RETURN COUNT(user)
            """
            print(query)
            count += self.query(query)[0]
        return count 
    
    @count_query_logging
    def connect_cast_recasts(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (user:User:Farcaster {{fid: rows.fid}})
            WITH user, rows.hash as hash
            MATCH (cast:Cast {{hash: hash}})
            MERGE (user)-[r:POSTED]->(cast)
            RETURN COUNT(user)
            """
            count += self.query(query)[0]
        return count 
    

    @count_query_logging
    def connect_additional_channel_memberships(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MATCH (user:User:Farcaster {{fid: rows.fid}})
            MATCH (channel:Channel {{channelId: rows.id}})
            MERGE (user)-[r:MEMBER]->(channel)
            RETURN COUNT(user)
            """
            count += self.query(query)[0]
        return count 