import os
import logging
import time
import re
import math
import sys
from datetime import datetime
from typing import Optional, Union, List, Dict, Any
import pandas as pd
from neo4j import BoltDriver, GraphDatabase, Neo4jDriver
from dotenv import load_dotenv
from functools import wraps
import json
import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO)

def count_query_logging(function):
    """Decorator for logging query counts"""
    @wraps(function)
    def wrapper(*args, **kwargs):
        logging.info(f"Ingesting with: {function.__name__}")
        count = function(*args, **kwargs)
        logging.info(f"Created or merged: {count}")
        return count
    return wrapper

def get_query_logging(function):
    """Decorator for logging query results"""
    @wraps(function)
    def wrapper(*args, **kwargs):
        logging.info(f"Getting data with: {function.__name__}")
        result = function(*args, **kwargs)
        logging.info(f"Objects retrieved: {len(result)}")
        return result
    return wrapper

class Cypher:
    """Base class for Neo4j database operations"""
    def __init__(self, database: Optional[str] = None) -> None:
        load_dotenv()
        self.database = database or os.getenv("NEO_DB")
        self.unique_id = datetime.timestamp(datetime.now())
        self.CREATED_ID = f"created:{self.unique_id}"
        self.UPDATED_ID = f"updated:{self.unique_id}"
        self.asOf = datetime.now().timestamp()

    def get_driver(self) -> Union[Neo4jDriver, BoltDriver]:
        """Create and return a Neo4j driver instance"""
        uri = os.getenv("NEO4J_URI")
        username = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASS")
        
        if not all([uri, username, password]):
            raise ValueError("Missing required Neo4j environment variables")
            
        return GraphDatabase.driver(uri, auth=(username, password))

    def run_query(self,
                  neo4j_driver: Neo4jDriver,
                  query: str,
                  parameters: Optional[Dict] = None,
                  counter: int = 0) -> Any:
        """Execute Neo4j query with retry logic"""
        time.sleep(counter * 10)
        assert neo4j_driver is not None, "Driver not initialized!"
        
        session = None
        try:
            session = neo4j_driver.session(database=self.database) if self.database else neo4j_driver.session()
            result = session.run(query, parameters)
            
            return result.single()
            
        except Exception as e:
            logging.error(f"Query failed: {e}")
            if counter > 10:
                raise e
            return self.run_query(neo4j_driver, query, parameters, counter + 1)
        finally:
            if session:
                session.close()
            neo4j_driver.close()

    def query(self,
              query: str,
              parameters: Optional[Dict] = None) -> Any:
        """Execute query and return results in default Neo4j format"""
        neo4j_driver = self.get_driver()
        return self.run_query(neo4j_driver, query, parameters)

    def sanitize_text(self, text: Optional[str]) -> str:
        """Clean text for safe database insertion"""
        if text:
            return text.rstrip().replace('\r', '').replace('\\', '').replace('"', '').replace("'", "").replace("`", "").replace("\n", "")
        return ""

class Ingestor:
    """Enhanced base class for data ingestion with S3 and Neo4j support"""
    def __init__(self, bucket_name: str):
        load_dotenv()
        
        # Basic initialization
        self.bucket_name = os.getenv("AWS_BUCKET_PREFIX", "") + bucket_name if not bucket_name.startswith("tc-") else bucket_name
        self.metadata_filename = "ingestor_metadata.json"
        self.runtime = datetime.now()
        
        # S3 specific initialization
        self.S3_max_size = 1000000000
        self.data = {}
        self.metadata = {}
        self.scraper_data = {}
        self.allow_override = os.getenv("ALLOW_OVERRIDE") == "1"
        self.start_date = None
        self.end_date = None
        
        # Initialize AWS clients
        self.AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
        self.AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        if not all([self.AWS_ACCESS_KEY_ID, self.AWS_SECRET_ACCESS_KEY]):
            raise ValueError("Missing AWS credentials in environment variables")
            
        self.s3_client = boto3.client(
            's3',
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY
        )
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.create_or_get_bucket()
        
        # Load initial data
        self.load_data()
        
        if not hasattr(self, 'cyphers'):
            raise NotImplementedError("Cyphers have not been instantiated to self.cyphers")

        # Print size of scraper_data
        logging.info(f"Size of scraper_data: {self.get_size(self.scraper_data)} bytes")

    def configure_bucket(self):
        """Configure bucket for public access and object ownership"""
        self.s3_client.put_public_access_block(
            Bucket=self.bucket_name,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': False,
                'IgnorePublicAcls': False,
                'BlockPublicPolicy': False,
                'RestrictPublicBuckets': False
            }
        )
        self.s3_client.put_bucket_ownership_controls(
            Bucket=self.bucket_name,
            OwnershipControls={
                'Rules': [{'ObjectOwnership': 'ObjectWriter'}]
            }
        )

    def create_or_get_bucket(self):
        """Create or get existing S3 bucket"""
        response = self.s3_client.list_buckets()
        if self.bucket_name not in [el["Name"] for el in response["Buckets"]]:
            try:
                logging.warning(f"Bucket not found! Creating {self.bucket_name}")
                location = {"LocationConstraint": os.getenv("AWS_DEFAULT_REGION", "us-east-1")}
                self.s3_client.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration=location)
                self.configure_bucket()
            except ClientError as e:
                logging.error(f"An error occurred during bucket creation: {e}")
                raise
        return self.s3_resource.Bucket(self.bucket_name)

    def get_size(self, obj: Union[Dict, List], seen=None) -> int:
        """Recursively calculate size of objects"""
        size = sys.getsizeof(obj)
        if seen is None:
            seen = set()
        obj_id = id(obj)
        if obj_id in seen:
            return 0
        seen.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self.get_size(v, seen) for v in obj.values()])
            size += sum([self.get_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self.get_size(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.get_size(i, seen) for i in obj])
        return size

    def split_dataframe(self, df: pd.DataFrame, chunk_size: int = 10000) -> List[pd.DataFrame]:
        """Split dataframe into smaller chunks"""
        chunks = []
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
        for i in range(num_chunks):
            chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
        return chunks

    def save_df_as_csv(self, df: pd.DataFrame, file_name: str, 
                      ACL='public-read', max_lines: int = 10000, 
                      max_size: int = 10000000) -> List[str]:
        """Save DataFrame to CSV in S3 with chunking support"""
        chunks = [df]
        if df.memory_usage(index=False).sum() > max_size or len(df) > max_lines:
            chunks = self.split_dataframe(df, chunk_size=max_lines)

        urls = []
        for chunk_id, chunk in enumerate(chunks):
            chunk_name = f"{file_name}--{chunk_id}.csv"
            chunk.to_csv(f"s3://{self.bucket_name}/{chunk_name}", index=False)
            self.s3_resource.ObjectAcl(self.bucket_name, chunk_name).put(ACL=ACL)
            location = self.s3_client.get_bucket_location(Bucket=self.bucket_name)["LocationConstraint"] or 'us-east-1'
            urls.append(f"https://s3-{location}.amazonaws.com/{self.bucket_name}/{chunk_name}")
        return urls

    def save_json_as_csv(self, data: List[Dict], file_name: str, 
                        ACL="public-read", max_lines: int = 10000,
                        max_size: int = 10000000) -> List[str]:
        """Convert JSON data to CSV and save to S3"""
        df = pd.DataFrame(data)
        return self.save_df_as_csv(df, file_name, ACL, max_lines, max_size)

    def get_most_recent_datafile(self) -> Optional[str]:
        """Get the most recent data file from S3"""
        datafiles = []
        for obj in self.bucket.objects.all():
            if obj.key.startswith("data_"):
                datafiles.append(obj.key)

        if not datafiles:
            return None

        date_pattern = re.compile(r"data_.*?(\d{4}[-_]\d{2}[-_]\d{2}[-_]\d{2}[-_]\d{2})")
        
        def extract_date(filename):
            match = date_pattern.search(filename)
            if match:
                date_str = re.sub(r"[^\d]", "-", match.group(1))
                return datetime.strptime(date_str, "%Y-%m-%d-%H-%M")
            return datetime.min

        return max(datafiles, key=extract_date)

    def load_data(self) -> None:
        """Load the most recent data file from S3"""
        most_recent_file = self.get_most_recent_datafile()
        if most_recent_file:
            logging.info(f"File found: {most_recent_file}")
            logging.info(f"Loading file: {most_recent_file}")
            self.scraper_data = self.load_json(most_recent_file)
        else:
            logging.warning("No data files found in S3 bucket.")

    def load_json(self, filename: str) -> Dict:
        """Load JSON data from S3"""
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=filename)
        return json.loads(obj['Body'].read().decode('utf-8'))

    def set_start_end_date(self) -> None:
        """Set start and end dates from environment or metadata"""
        if not self.start_date and "INGEST_FROM_DATE" in os.environ:
            self.start_date = os.getenv("INGEST_FROM_DATE")
        elif "last_date_ingested" in self.metadata:
            self.start_date = self.metadata["last_date_ingested"]
            
        if not self.end_date and "INGEST_TO_DATE" in os.environ:
            self.end_date = os.getenv("INGEST_TO_DATE")
            
        if self.start_date:
            self.start_date = datetime.strptime(self.start_date, "%Y-%m-%d-%H-%M")
        if self.end_date:
            self.end_date = datetime.strptime(self.end_date, "%Y-%m-%d-%H-%M")

    def run(self) -> None:
        """Main ingestion method - must be implemented by subclasses"""
        raise NotImplementedError("The run function has not been implemented!")