import json
import os
import time
from pathlib import Path

import dagster as dg
from dagster_async_executor import async_executor

from dagster_threaded_executor import threaded_executor

WAIT_SQL = Path(__file__).with_name("wait.sql").read_text()


@dg.resource
def bigquery_credentials():
    import google.auth
    from google.auth.transport.requests import Request

    credentials, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    assert project
    credentials.refresh(Request())
    return credentials, project


def query_and_wait(credentials, project):
    from google.api_core.retry import Retry
    from google.cloud import bigquery

    start = time.monotonic()
    retry = Retry(timeout=30)
    with bigquery.Client(
        project=project, credentials=credentials, location="US"
    ) as client:
        job = client.query(
            WAIT_SQL,
            job_config=bigquery.QueryJobConfig(
                use_query_cache=False, job_timeout_ms=30_000
            ),
            api_method=bigquery.enums.QueryApiMethod.INSERT,
            timeout=20,
            retry=retry,
            job_retry=None,
        )
        submitted = time.monotonic()
        deadline = submitted + 60
        while job.state != "DONE":
            if time.monotonic() >= deadline:
                raise TimeoutError("BigQuery job did not finish")
            time.sleep(0.75)
            job.reload(retry=retry, timeout=20)
        assert not job.error_result
        assert job.total_bytes_processed == job.total_bytes_billed == 0
        server_started = job.started.timestamp()
        server_ended = job.ended.timestamp()
        assert server_ended - server_started >= 5
    return (
        start,
        submitted,
        time.monotonic(),
        server_started,
        server_ended,
        job.slot_millis,
        job.total_bytes_billed,
    )


def write_interval(context, interval):
    with Path(os.environ["PROFILE_OPS_FILE"]).open("a", encoding="utf-8") as output:
        output.write(json.dumps((*interval, os.getpid(), context.op.name)) + "\n")


@dg.op(required_resource_keys={"bigquery_credentials"}, out=dg.Out(dg.Nothing))
def sync_query(context):
    write_interval(context, query_and_wait(*context.resources.bigquery_credentials))


mode = os.environ["PROFILE_MODE"]
executor = {
    "threaded": threaded_executor,
    "async-sync": async_executor,
    "multiprocess": dg.multiprocess_executor,
}[mode]


@dg.job(
    executor_def=executor, resource_defs={"bigquery_credentials": bigquery_credentials}
)
def bigquery_job():
    for index in range(int(os.environ["PROFILE_STEPS"])):
        sync_query.alias(f"query_{index:02d}")()
