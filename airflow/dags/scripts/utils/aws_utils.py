import json
import boto3
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


def get_boto3_session(
        region_name: str = "eu-north-1", 
        profile_name: str | None = None
        ) -> boto3.Session:
    """
    Create boto3 session
    
    Args:
        region_name: AWS region
        
    Returns:
        boto3 Session object
    """
    if profile_name:
        return boto3.Session(profile_name=profile_name, region_name=region_name)
    return boto3.Session(region_name=region_name)

def get_parameter_from_ssm(
        parameter_name: str, 
        region_name: str = "eu-north-1",
        profile_name: str | None = None,
        parse_json: bool = True
        ) -> str:
    """
    Get parameter from AWS Systems Manager Parameter Store
    
    Args:
        parameter_name: Name of the parameter
        region_name: AWS region
        
    Returns:
        Parameter value as string
    """
    try:
        session = get_boto3_session(region_name, profile_name=profile_name)
        client = session.client('ssm')
        
        response = client.get_parameter(
            Name=parameter_name, 
            WithDecryption=True
            )
        
        value = response['Parameter']['Value']
        logger.info(f"Retrieved parameter: {parameter_name}")
        if parse_json:
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                pass  

        return value
        
    except Exception as e:
        logger.error(f"Error retrieving parameter {parameter_name}: {str(e)}")
        raise
        

def put_parameter_in_ssm(
    secret_name: str,
    secret_value: Dict,
    region_name: str = "eu-north-1",
    profile_name: str | None = None
) -> bool:
    """
    Create or update secreSt in AWS Secrets Manager
    
    Args:
        secret_name: Name for the secret
        secret_value: Dictionary of secret values
        region_name: AWS region
        
    Returns:
        True if successful
    """
    try:
        session = get_boto3_session(region_name, profile_name=profile_name)
        client = session.client('ssm')
        
        client.put_parameter(
            Name=secret_name,
            Value=json.dumps(secret_value),
            Type='SecureString',
            Overwrite=True
        )
        logger.info(f"Created paramter: {secret_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error storing parameter {secret_name}: {str(e)}")
        raise


def get_parameters_by_path(
        path: str, 
        region_name: str = "eu-north-1",
        profile_name: str | None = None
        ) -> Dict[str, str]:
    """
    Get all parameters under a path from SSM Parameter Store
    
    Args:
        path: Parameter path (e.g., /coretelecoms/hassan/db/)
        region_name: AWS region
        
    Returns:
        Dictionary of parameter names and values
    """
    try:
        session = get_boto3_session(region_name, profile_name=profile_name)
        client = session.client('ssm')
        
        parameters = {}
        paginator = client.get_paginator('get_parameters_by_path')
        
        for page in paginator.paginate(Path=path, Recursive=True, WithDecryption=True):
            for param in page['Parameters']:
                key = param['Name'].replace(path, '').lstrip('/')
                parameters[key] = param['Value']
        
        logger.info(f"Retrieved {len(parameters)} parameters from {path}")
        return parameters
        
    except Exception as e:
        logger.error(f"Error retrieving parameters from {path}: {str(e)}")
        raise



def parse_json_parameters(
    parameters: Dict[str, str],
    json_keys: list[str]
) -> Dict[str, Any]:
    """
    Parse specific parameters as JSON
    
    Args:
        parameters: Raw dict from get_parameters_by_path
        json_keys: List of keys that should be parsed as JSON
        
    Returns:
        Copy of parameters with specified keys parsed as dict/list
    """
    parsed = parameters.copy()
    for key in json_keys:
        if key in parsed:
            try:
                parsed[key] = json.loads(parsed[key])
                logger.info(f"Parsed parameter '{key}' as JSON")
            except json.JSONDecodeError as e:
                logger.warning(f"Parameter '{key}' is not valid JSON: {e}")
    return parsed


def list_s3_objects(
        bucket: str, 
        prefix: str = "", 
        suffix: str = "", 
        region_name: str = "eu-north-1",
        profile_name: str | None = None
        ) -> List[str]:
    """
    List objects in S3 bucket
    
    Args:
        bucket: S3 bucket name
        prefix: Prefix to filter objects
        suffix: Suffix to filter objects (e.g., '.csv')
        region_name: AWS region
        
    Returns:
        List of object keys
    """
    try:
        session = get_boto3_session(region_name, profile_name=profile_name)
        client = session.client('s3')
        
        objects = []
        paginator = client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if suffix == "" or key.endswith(suffix):
                        objects.append(key)
        
        logger.info(f"Found {len(objects)} objects in s3://{bucket}/{prefix}")
        return objects
        
    except Exception as e:
        logger.error(f"Error listing objects in {bucket}/{prefix}: {str(e)}")
        raise


def upload_to_s3(
        local_path: str, 
        bucket: str, 
        s3_key: str, 
        region_name: str = "eu-north-1",
        profile_name: str | None = None
        ) -> bool:
    """
    Upload file to S3
    
    Args:
        local_path: Local file path
        bucket: S3 bucket name
        s3_key: S3 object key
        region_name: AWS region
        
    Returns:
        True if successful
    """
    try:
        session = get_boto3_session(region_name, profile_name=profile_name)
        client = session.client('s3')
        
        client.upload_file(local_path, bucket, s3_key)
        logger.info(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise