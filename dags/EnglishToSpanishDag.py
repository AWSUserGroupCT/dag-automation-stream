from datetime import datetime
import boto3
import os
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator

AWS_REGION = "us-east-1"
SOURCE_BUCKET = "s3-towncenter-dags"
SOURCE_PREFIX = "english/"
TARGET_PREFIX = "spanish/"
SOURCE_LANG = "en"
TARGET_LANG = "es"

def translate_files_from_s3():
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    translate_client = boto3.client("translate", region_name=AWS_REGION)

    # List all objects in the english/ prefix
    response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX)

    if "Contents" not in response:
        print("No files found in source prefix.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith("/"):  # skip folders
            continue

        print(f"Processing file: {key}")
        # Download the file locally
        with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as tmp_file:
            s3_client.download_fileobj(SOURCE_BUCKET, key, tmp_file)
            tmp_file_path = tmp_file.name

        # Read content as text
        with open(tmp_file_path, "r", encoding="utf-8") as f:
            text = f.read()

        # Translate text
        translation = translate_client.translate_text(
            Text=text,
            SourceLanguageCode=SOURCE_LANG,
            TargetLanguageCode=TARGET_LANG
        )

        translated_text = translation["TranslatedText"]

        # Upload translated content to new prefix
        target_key = key.replace(SOURCE_PREFIX, TARGET_PREFIX, 1)
        s3_client.put_object(
            Bucket=SOURCE_BUCKET,
            Key=target_key,
            Body=translated_text.encode("utf-8")
        )

        print(f"Uploaded translated file to: {target_key}")

        # Cleanup temp file
        os.remove(tmp_file_path)


with DAG(
    dag_id="translate_english_to_spanish",
    description="Translate files from S3 english/ to spanish/ using Amazon Translate",
    start_date=datetime(2025, 8, 9),
    schedule_interval=None,  # run manually or set a cron
    catchup=False,
    tags=["aws", "translate", "s3"]
) as dag:

    translate_task = PythonOperator(
        task_id="translate_s3_files",
        python_callable=translate_files_from_s3
    )

    translate_task