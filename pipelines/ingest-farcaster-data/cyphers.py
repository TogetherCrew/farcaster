from tqdm import tqdm
from .helpers import get_query_logging, count_query_logging, Cypher


class FarcasterCyphers(Cypher):
    def __init__(self):
        super().__init__()



    @count_query_logging
    def test(self):
        query = """create (x) return count(x)"""
        results = self.query(query)
        print(results) 
        
