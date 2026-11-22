"""
"""
import os

import functions_framework
import google.auth
from google.cloud import bigquery

# Project is taken from the PROJECT_ID env var, else from the runtime credentials.
PROJECT_ID = os.environ.get("PROJECT_ID") or google.auth.default()[1]
DATASET = "hr_dataset"
TABLE = "fct_hr_snapshot_unpartitioned"
BUCKET = f"{PROJECT_ID}-hr-uploads"
TARGET_FILE_NAME = "fct_hr_snapshot.csv"

bq_client = bigquery.Client(project=PROJECT_ID)


@functions_framework.http
def load_hr_snapshot(request):
    uri = f"gs://{BUCKET}/{TARGET_FILE_NAME}"
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

    message = f"Loaded {load_job.output_rows} rows from {uri} into {table_ref}"
    print(message)
    return {"rows_loaded": load_job.output_rows, "uri": uri, "table": table_ref}, 200
