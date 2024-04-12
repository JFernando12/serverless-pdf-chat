import os, json
import boto3
from botocore.config import Config
import shortuuid
from aws_lambda_powertools import Logger
import requests
import base64

BUCKET = os.environ["BUCKET"]
REGION = os.environ["REGION"]

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://s3.{REGION}.amazonaws.com",
    config=Config(
        s3={"addressing_style": "virtual"}, region_name=REGION, signature_version="s3v4"
    ),
)
logger = Logger()

def s3_key_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, context):
    event_body = json.loads(event["body"])
    file_name_full = event_body["fileName"]
    file_name = file_name_full.split(".pdf")[0]
    base64_pdf = event_body["base64Pdf"]

    user_id = "74d8f4c8-30a1-709b-3c66-2b9a189aca33"

    exists = s3_key_exists(BUCKET, f"{user_id}/{file_name_full}/{file_name_full}")

    logger.info(
        {
            "user_id": user_id,
            "file_name_full": file_name_full,
            "file_name": file_name,
            "exists": exists,
        }
    )

    if exists:
        suffix = shortuuid.ShortUUID().random(length=4)
        key = f"{user_id}/{file_name}-{suffix}.pdf/{file_name}-{suffix}.pdf"
    else:
        key = f"{user_id}/{file_name}.pdf/{file_name}.pdf"

    pdfDecoded = base64.b64decode(base64_pdf)

    s3.put_object(Bucket=BUCKET, Key=key, Body=pdfDecoded, ContentType="application/pdf")

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps({ "success": True }),
    }
