"""
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# The Cloud Run URL is kept out of the code: set it as an Airflow Variable
# (Admin -> Variables) named "hr_snapshot_function_url".
FUNCTION_URL_VARIABLE = "hr_snapshot_function_url"


def trigger_load_hr_snapshot(**_) -> None:
    """Calls the Cloud Run function with an authenticated ID token --
    the standard way one GCP service calls another private Cloud Run
    service. Raises (failing the task) on any non-2xx response, e.g.
    if fct_hr_snapshot.csv hasn't been uploaded to the bucket yet."""
    import google.auth.transport.requests
    import google.oauth2.id_token
    import requests
    from airflow.models import Variable

    function_url = Variable.get(FUNCTION_URL_VARIABLE)

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, function_url)

    response = requests.post(
        function_url,
        headers={"Authorization": f"Bearer {id_token}"},
        timeout=120,
    )
    response.raise_for_status()
    print(response.json())


with DAG(
    dag_id="hr_snapshot_pipeline",
    description="Load fct_hr_snapshot.csv from GCS into BigQuery via the load-hr-snapshot Cloud Run function.",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "Aeljirai", "retries": 1},
    tags=["pipeline", "hr"],
) as dag:

    trigger_load_hr_snapshot_task = PythonOperator(
        task_id="trigger_load_hr_snapshot",
        python_callable=trigger_load_hr_snapshot,
    )
