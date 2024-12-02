import boto3
from boto3.s3.transfer import TransferConfig, S3Transfer
from tqdm import tqdm
import os

class ProgressBar(tqdm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def __call__(self, bytes_amount):
        self.update(bytes_amount)

def upload_file_with_progress(file_path, bucket_name, s3_key):
    s3_client = boto3.client('s3')
    file_size = os.path.getsize(file_path)
    progress_bar = ProgressBar(total=file_size, unit='B', unit_scale=True, desc=f"Uploading {os.path.basename(file_path)}")
    config = TransferConfig(multipart_threshold=1024 * 1024 * 5, multipart_chunksize=1024 * 1024 * 5)
    transfer = S3Transfer(client=s3_client, config=config)
    transfer.upload_file(file_path, bucket_name, s3_key, callback=progress_bar)
    progress_bar.close()
    print(f"Upload complete: s3://{bucket_name}/{s3_key}")

if __name__ == "__main__":
    local_file_path = "path"
    bucket_name = "s3_bucket"
    s3_key = "s3_key"
    upload_file_with_progress(local_file_path, bucket_name, s3_key)
