import os
import logging
import time
from datetime import datetime
from typing import Optional, Union, List, Dict, Any
import pandas as pd
from neo4j import BoltDriver, GraphDatabase, Neo4jDriver
from dotenv import load_dotenv
from functools import wraps
import json
import boto3
from botocore.exceptions import ClientError

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
                  response_format: str = 'df',
                  counter: int = 0) -> Union[pd.DataFrame, List[Dict]]:
        """Execute Neo4j query with retry logic"""
        time.sleep(counter * 10)
        assert neo4j_driver is not None, "Driver not initialized!"
        
        session = None
        try:
            session = neo4j_driver.session(database=self.database) if self.database else neo4j_driver.session()
            result = session.run(query, parameters)
            
            if response_format == 'json':
                response = [record.data() for record in result]
            else:  # default to 'df'
                response = pd.DataFrame([record.values() for record in result], columns=result.keys())
                
            return response
            
        except Exception as e:
            logging.error(f"Query failed: {e}")
            if counter > 10:
                raise e
            return self.run_query(neo4j_driver, query, parameters, response_format, counter + 1)
        finally:
            if session:
                session.close()
            neo4j_driver.close()

    def query(self,
              query: str,
              parameters: Optional[Dict] = None,
              response_format: str = 'df') -> Union[pd.DataFrame, List[Dict]]:
        """Execute query and return results in specified format"""
        neo4j_driver = self.get_driver()
        return self.run_query(neo4j_driver, query, parameters, response_format)

    def sanitize_text(self, text: Optional[str]) -> str:
        """Clean text for safe database insertion"""
        if text:
            return text.rstrip().replace('\r', '').replace('\\', '').replace('"', '').replace("'", "").replace("`", "").replace("\n", "")
        return ""

class Ingestor:
    """Base class for data ingestion with S3 and Neo4j support"""
    def __init__(self, bucket_name: str):
        load_dotenv()
        
        self.bucket_name = bucket_name
        self.metadata_filename = "ingestor_metadata.json"
        self.runtime = datetime.now()
        
        # Initialize S3 client with env variables
        self.AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
        self.AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        if not all([self.AWS_ACCESS_KEY_ID, self.AWS_SECRET_ACCESS_KEY]):
            raise ValueError("Missing AWS credentials in environment variables")
            
        self.s3_client = boto3.client(
            's3',
            region_name='us-east-1',
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY
        )
        
        self.load_metadata()
        
        if not hasattr(self, 'cyphers'):
            raise NotImplementedError("Cyphers have not been instantiated to self.cyphers")

    def load_metadata(self) -> None:
        """Load metadata from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.metadata_filename)
            self.metadata = json.loads(response['Body'].read().decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.metadata = {}
            else:
                raise

    def save_metadata(self) -> None:
        """Save current metadata to S3"""
        self.metadata["last_date_ingested"] = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"
        self.save_json(self.metadata_filename, self.metadata)

    def save_json(self, filename: str, data: Dict) -> str:
        """Save JSON data to S3 and return URL"""
        try:
            json_data = json.dumps(data)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=json_data.encode('utf-8')
            )
            return f"s3://{self.bucket_name}/{filename}"
        except Exception as e:
            logging.error(f"Error saving to S3: {e}")
            raise

    def save_json_as_csv(self, data: List[Dict], base_filename: str) -> str:
        """Convert JSON data to CSV and save to S3"""
        try:
            df = pd.DataFrame(data)
            csv_filename = f"{base_filename}.csv"
            csv_buffer = df.to_csv(index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=csv_filename,
                Body=csv_buffer
            )
            return f"s3://{self.bucket_name}/{csv_filename}"
        except Exception as e:
            logging.error(f"Error saving CSV to S3: {e}")
            raise

    def save_df_as_csv(self, df: pd.DataFrame, base_filename: str) -> str:
        """Save DataFrame as CSV to S3"""
        try:
            csv_filename = f"{base_filename}.csv"
            csv_buffer = df.to_csv(index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=csv_filename,
                Body=csv_buffer.encode('utf-8')
            )
            return f"s3://{self.bucket_name}/{csv_filename}"
        except Exception as e:
            logging.error(f"Error saving DataFrame as CSV to S3: {e}")
            raise

    def run(self) -> None:
        """Main ingestion method - must be implemented by subclasses"""
        raise NotImplementedError("The run function has not been implemented!")