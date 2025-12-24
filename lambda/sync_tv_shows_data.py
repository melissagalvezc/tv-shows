#!/usr/bin/env python3
import os
import json
import logging
import datetime
import requests
import boto3
import re
from typing import Dict, List, Optional
from botocore.exceptions import ClientError

# Load .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


TV_SHOWS_API_URL = "https://api.tvmaze.com/shows"

S3_BUCKET = os.environ.get("S3_BUCKET", "tv-shows-data")
S3_PREFIX = "tv_shows_data"
METADATA_S3_KEY = f"{S3_PREFIX}/metadata/tv_shows_sync_metadata.json"


class TVShowsDataSync:
    def __init__(self, aws_region: Optional[str] = None):
        """Initialize TVShowsDataSync.
        
        Args:
            aws_region: AWS region (
        """
        self.session = requests.Session()
        self.timeout = 30
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # Initialize S3 client - uses IAM Role automatically in Lambda
        s3_config = {}
        if aws_region:
            s3_config['region_name'] = aws_region
        else:
            s3_config['region_name'] = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        
        self.s3_client = boto3.client('s3', **s3_config)
        self.s3_cached_objects = {}
        
    def _load_metadata(self) -> Dict:
        """Load sync metadata from S3."""
        try:
            response = self.s3_client.get_object(Bucket=S3_BUCKET, Key=METADATA_S3_KEY)
            metadata = json.loads(response['Body'].read().decode('utf-8'))
            return metadata
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return {
                    "last_sync": None
                }
            else:
                logger.error(f"Error loading metadata from S3: {str(e)}")
                raise
            
    def _save_metadata(self, metadata: Dict) -> None:
        """Save sync metadata to S3."""
        metadata["last_sync"] = datetime.datetime.now().isoformat()
        metadata_json = json.dumps(metadata, indent=2)
        self.s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=METADATA_S3_KEY,
            Body=metadata_json.encode('utf-8'),
            ContentType='application/json'
        )
    
    def _normalize_keys(self, data: Dict) -> Dict:
        """Convert keys to snake_case."""
        normalized = {}
        for key, value in data.items():
            clean_key = key.strip()
            snake_key = re.sub(r'[^\w\s]', '', clean_key)
            snake_key = re.sub(r'\s+', '_', snake_key)
            snake_key = snake_key.lower()
            
            if not snake_key:
                logger.warning(f"Key '{key}' would be empty after normalization. Using original.")
                snake_key = key
                
            normalized[snake_key] = value
        return normalized
    
    def _check_s3_object_exists(self, s3_key: str) -> bool:
        """Check if S3 object exists."""
        try:
            self.s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking S3 object {s3_key}: {str(e)}")
                raise
                
    def _list_s3_objects(self, prefix: str) -> List[str]:
        """List S3 objects by prefix."""
        try:
            if prefix in self.s3_cached_objects:
                return self.s3_cached_objects[prefix]
                
            objects = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            response_iterator = paginator.paginate(
                Bucket=S3_BUCKET,
                Prefix=prefix
            )
            
            for page in response_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects.append(obj['Key'])
            
            self.s3_cached_objects[prefix] = objects
            return objects
        except Exception as e:
            logger.error(f"Error listing S3 objects with prefix {prefix}: {str(e)}")
            return []

    def fetch_data(self, backfill: bool = False) -> Dict:
        """Fetch data from TV Shows API."""
        logger.info("Starting TV Shows data fetch")
        
        metadata = self._load_metadata()
        
        try:
            api_prefix = f"{S3_PREFIX}/api/"
            api_objects = self._list_s3_objects(api_prefix)
            
            use_cached_api = False
            api_data = None
            
            if not backfill and api_objects:
                latest_api_object = sorted(api_objects)[-1]
                
                yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
                
                date_match = re.search(r'load_date=(\d{4}/\d{2}/\d{2})', latest_api_object)
                if date_match:
                    date_str = date_match.group(1)
                    try:
                        object_date = datetime.datetime.strptime(date_str, "%Y/%m/%d")
                        if object_date >= yesterday:
                            use_cached_api = True
                            logger.info(f"Using cached API response from {latest_api_object}")
                            
                            response = self.s3_client.get_object(Bucket=S3_BUCKET, Key=latest_api_object)
                            api_data = json.loads(response['Body'].read().decode('utf-8'))
                    except ValueError:
                        pass
            
            if not use_cached_api:
                logger.info("Fetching fresh data from TV Shows API")
                response = self.session.get(TV_SHOWS_API_URL, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                
                normalized_data = {
                    "shows": [],
                    "fetched_at": datetime.datetime.now().isoformat()
                }
                
                if isinstance(data, list):
                    for show in data:
                        if show:
                            normalized_show = self._normalize_keys(show)
                            normalized_data["shows"].append(normalized_show)
                
                api_data = normalized_data
            
            logger.info(f"Fetched {len(api_data.get('shows', []))} shows from API")
            
            # Save metadata after successful fetch
            self._save_metadata(metadata)
            
            return api_data
            
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            raise

    def upload_to_s3(self, api_data: Dict):
        """Upload the processed data to S3 bucket."""
        try:
            current_date = datetime.datetime.now().strftime("%Y/%m/%d")
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            
            logger.info("Uploading API data to S3")
            s3_key = f"{S3_PREFIX}/api/load_date={current_date}/tv_shows_api_{timestamp}.json"
            
            # Check if file already exists
            if self._check_s3_object_exists(s3_key):
                logger.info(f"Skipping {s3_key} - already exists")
            else:
                api_json = json.dumps(api_data, indent=2)
                self.s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=api_json.encode('utf-8'),
                    ContentType='application/json'
                )
                logger.info(f"Uploaded API data to {s3_key}")
            
            logger.info("Data upload to S3 completed successfully")
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            raise

def lambda_handler(event, context):
    """AWS Lambda handler - entry point for Lambda execution."""
    try:
        backfill = os.environ.get("BACKFILL", "false").lower() == "true"
        aws_region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
        
        data_sync = TVShowsDataSync(aws_region=aws_region)
        api_data = data_sync.fetch_data(backfill=backfill)
        data_sync.upload_to_s3(api_data)
        
        logger.info("TV Shows data sync completed successfully")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'TV Shows data sync completed successfully'
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

def main():
    """Main entry point for local execution."""
    try:
        backfill = os.environ.get("BACKFILL", "false").lower() == "true"
        aws_region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
        
        data_sync = TVShowsDataSync(aws_region=aws_region)
        api_data = data_sync.fetch_data(backfill=backfill)
        data_sync.upload_to_s3(api_data)
        
        logger.info("TV Shows data sync completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()

