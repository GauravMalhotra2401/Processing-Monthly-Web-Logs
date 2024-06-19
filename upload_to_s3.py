import boto3

s3 = boto3.client("s3")
bucket_name = "web-logs-bucket-monthly"


def upload_to_s3(filename,csv_buffer):
    file_path = f"{filename}"  
    
    s3.upload_fileobj(csv_buffer, bucket_name, file_path)
    print(f"File uploaded to S3 Bucket {bucket_name}/{file_path}")
    return