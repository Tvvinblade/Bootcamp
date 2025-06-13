import boto3
from datetime import datetime
from typing import List, Optional


class S3FileManager:
    def __init__(self, bucket_name: str, aws_access_key: str, aws_secret_key: str, endpoint_url: str = None):
        self.bucket_name = bucket_name
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            endpoint_url=endpoint_url
        )

    def list_files_newer_than(self, prefix: str, update_at: datetime) -> List[str]:
        paginator = self.s3.get_paginator('list_objects_v2')
        new_files = []

        if update_at is not None:
            unix_time_ms = int(update_at.timestamp() * 1000)
        else:
            unix_time_ms = 0

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            print(f"Found {len(page.get('Contents', []))} objects in page")  # Debug
            for obj in page.get('Contents', []):
                print(f"Checking object: {obj['Key']}, LastModified: {obj['LastModified']}")
                last_modified_unix = int(obj['LastModified'].timestamp() * 1000)

                if last_modified_unix > unix_time_ms:
                    path = f"s3a://{self.bucket_name}/{obj['Key']}"
                    print(f"Adding: {path}")
                    new_files.append(path)

        return new_files

