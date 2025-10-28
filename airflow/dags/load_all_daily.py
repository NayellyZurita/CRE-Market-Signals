from __future__ import annotations

import os
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "cre-market-signals")
DOCKER_NETWORK = f"{COMPOSE_PROJECT}_default"
API_IMAGE = os.environ.get("API_IMAGE", "cre-market-signals-api:latest")
DATA_MOUNT = Mount(target="/app/data", source="market_signals_data", type="volume")

ENV_KEYS = [
    "HUD_TOKEN",
    "FRED_API_KEY",
    "CENSUS_API_KEY",
    "DEFAULT_GEO",
    "DEFAULT_START_YEAR",
    "DEFAULT_END_YEAR",
    "DEFAULT_YEAR",
    "FRED_OBSERVATION_START",
    "FRED_OBSERVATION_END",
]

ENVIRONMENT = {key: value for key in ENV_KEYS if (value := os.environ.get(key))}

QUALITY_CHECK_SCRIPT = dedent(
    """
from storage.db import connect

conn = connect(read_only=True)
counts = {
    "total": conn.execute("SELECT COUNT(*) FROM market_signals").fetchone()[0],
    "hud": conn.execute("SELECT COUNT(*) FROM market_signals WHERE source='hud_fmr'").fetchone()[0],
    "acs": conn.execute("SELECT COUNT(*) FROM market_signals WHERE source='acs'").fetchone()[0],
    "fred": conn.execute("SELECT COUNT(*) FROM market_signals WHERE source='fred'").fetchone()[0],
}
conn.close()

assert counts["total"] > 0, "No rows loaded"
assert counts["hud"] >= 5, "Unexpected HUD row count"
assert counts["acs"] >= 4, "Unexpected ACS row count"
assert counts["fred"] >= 1, "Unexpected FRED row count"
print(counts)
    """
).strip()

RECORD_STATUS_SCRIPT = dedent(
    """
from datetime import datetime, timezone
from storage.db import connect

conn = connect()
conn.execute(
    "CREATE TABLE IF NOT EXISTS load_status (status_key TEXT PRIMARY KEY, loaded_at TIMESTAMP)"
)
conn.execute(
    "INSERT OR REPLACE INTO load_status VALUES (?, ?)",
    ("load_all_daily", datetime.now(timezone.utc)),
)
conn.close()
    """
).strip()

with DAG(
    dag_id="load_all_daily",
    description="Run jobs.load_all to refresh market signals and record status",
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["market-signals", "etl"],
) as dag:

    load_market_signals = DockerOperator(
        task_id="load_market_signals",
        image=API_IMAGE,
        command=["python", "-m", "jobs.load_all"],
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        network_mode=DOCKER_NETWORK,
        environment=ENVIRONMENT,
        mounts=[DATA_MOUNT],
        mount_tmp_dir=False,
        do_xcom_push=False,
    )

    data_quality_checks = DockerOperator(
        task_id="data_quality_checks",
        image=API_IMAGE,
        command=["python", "-c", QUALITY_CHECK_SCRIPT],
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        network_mode=DOCKER_NETWORK,
        environment=ENVIRONMENT,
        mounts=[DATA_MOUNT],
        mount_tmp_dir=False,
        do_xcom_push=False,
    )

    record_status = DockerOperator(
        task_id="record_last_success",
        image=API_IMAGE,
        command=["python", "-c", RECORD_STATUS_SCRIPT],
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        network_mode=DOCKER_NETWORK,
        environment=ENVIRONMENT,
        mounts=[DATA_MOUNT],
        mount_tmp_dir=False,
        do_xcom_push=False,
    )

    load_market_signals >> data_quality_checks >> record_status
