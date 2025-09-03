#!/usr/bin/env python3
import os, sys, mimetypes, boto3, botocore

def need(name):
    v = os.environ.get(name)
    if not v:
        print(f"Missing env var: {name}", file=sys.stderr)
        sys.exit(2)
    return v

def get_s3():
    account = need("R2_ACCOUNT_ID")
    key = need("R2_ACCESS_KEY_ID")
    secret = need("R2_SECRET_ACCESS_KEY")
    endpoint = f"https://{account}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name="auto",
        config=botocore.config.Config(signature_version="s3v4"),
    )

def push(file_path, sku, dated_name, bucket=None):
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)
    s3 = get_s3()
    bucket = bucket or need("R2_BUCKET")
    ctype, _ = mimetypes.guess_type(file_path)
    extra = {"ContentType": ctype} if ctype else {}
    key_dated  = f"{sku}/{dated_name}"
    key_latest = f"{sku}/latest"
    s3.upload_file(file_path, bucket, key_dated, ExtraArgs=extra)
    s3.upload_file(file_path, bucket, key_latest, ExtraArgs=extra)
    return key_dated, key_latest

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: r2_push.py <file_path> <SKU> <dated_name>", file=sys.stderr)
        sys.exit(1)
    kd, kl = push(sys.argv[1], sys.argv[2], sys.argv[3])
    print("OK ->", kd, kl)
