#!/usr/bin/env python3

import os
import sys
import argparse
import boto3

CORS_CONFIGURATION = {
    'CORSRules': [{
        "AllowedHeaders": [],
        "AllowedMethods": ["GET", "HEAD"],
        "AllowedOrigins": ["*"],
        "ExposeHeaders": []
    }]
}

LIFECYCLE_POLICY = {
    "Rules": [{
        "ID": "remove-old-r2e2-jobs",
        "Expiration": {
            "Days": 7
        },
        "Filter": {
            "Prefix": "jobs/"
        },
        "Status": "Enabled",
        "NoncurrentVersionExpiration": {
            "NoncurrentDays": 7
        },
        "AbortIncompleteMultipartUpload": {
            "DaysAfterInitiation": 7
        }
    }]
}


def create_bucket(name, region):
    client = boto3.client('s3', region_name=region)

    print(f">> Creating bucket '{name}' in {region}... ", end="")
    client.create_bucket(
        ACL='private',
        Bucket=name,
        CreateBucketConfiguration={'LocationConstraint': region},
    )
    print("done.")

    print(f">> Setting CORS configuration... ", end="")
    client.put_bucket_cors(Bucket=name, CORSConfiguration=CORS_CONFIGURATION)
    print("done.")

    print(f">> Create lifecycle configuration for 'jobs/' prefix... ", end="")
    client.put_bucket_lifecycle_configuration(Bucket=name, LifecycleConfiguration=LIFECYCLE_POLICY)
    print("done.")


def main():
    parser = argparse.ArgumentParser(
        description="Create a bucket for R2E2 jobs")
    parser.add_argument('name', help='bucket name')
    parser.add_argument('region', help='bucket region')
    args = parser.parse_args()

    create_bucket(args.name, args.region)


if __name__ == "__main__":
    main()
