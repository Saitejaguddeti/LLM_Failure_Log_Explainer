import pytest
from conftest import SAMPLES

from log_explainer.patterns import find_matches
from log_explainer.redact import redact


@pytest.mark.parametrize(
    "name,expected",
    [
        ("airflow_task_timeout.log", "airflow_task_timeout"),
        ("airflow_db_auth_failure.log", "auth_failed"),
        ("spark_executor_oom.log", "spark_executor_oom"),
        ("spark_analysis_exception.log", "spark_unresolved"),
    ],
)
def test_each_sample_matches_its_pattern_first(name, expected):
    lines = (SAMPLES / name).read_text().splitlines()
    assert find_matches(lines)[0].signature.id == expected


@pytest.mark.parametrize(
    "line,expected",
    [
        ("Caused by: org.apache.spark.shuffle.FetchFailedException: Failed to connect to ip-10-0-1-5", "spark_fetch_failed"),
        ("org.apache.spark.SparkException: Could not execute broadcast in 300 secs.", "spark_broadcast_timeout"),
        ("ModuleNotFoundError: No module named 'snowflake'", "import_error"),
        ("airflow.exceptions.AirflowSensorTimeout: Sensor has timed out; run duration of 3600.2 seconds", "airflow_sensor_timeout"),
        ("[2026-10-12, 03:00:00 UTC] {local_task_job_runner.py:228} INFO - Task exited with return code Negsignal.SIGKILL", "killed_externally"),
        ("snowflake.connector.errors.ProgrammingError: 002003 (42S02): SQL compilation error: Object 'CURATED.TRADES_V2' does not exist or not authorized.", "snowflake_sql_error"),
        ("OSError: [Errno 28] No space left on device", "disk_full"),
        ("psycopg2.OperationalError: could not connect to server: Connection refused", "connection_failed"),
        ("botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the GetObject operation", "access_denied"),
        ("checks.DataQualityError: row count check failed for curated.trades: 0 rows", "data_quality"),
    ],
)  # fmt: skip
def test_single_lines(line, expected):
    assert find_matches([line])[0].signature.id == expected


def test_routine_lines_match_nothing():
    lines = ["INFO - Task exited with return code 0", "INFO SparkContext: Successfully stopped SparkContext"]
    assert find_matches(lines) == []


def test_password_in_a_connection_string_is_masked():
    text, counts = redact("Connecting to postgresql://etl_user:Sup3rS3cret!@positions-db.internal:5432/positions")
    assert text == "Connecting to postgresql://etl_user:[REDACTED]@positions-db.internal:5432/positions"
    assert counts["url_password"] == 1


def test_keys_tokens_and_secrets_are_masked():
    raw = (
        "AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY "
        "curl -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.e30.c2lnbmF0dXJl' https://api.example.com AUTH_TOKEN=abc123def"
    )
    text, _ = redact(raw)
    for secret in ["AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI", "eyJhbGciOiJIUzI1NiJ9", "abc123def"]:
        assert secret not in text


def test_emails_and_account_like_numbers_are_masked():
    text, counts = redact("Sending alert email to data-eng-alerts@acme.example about account 123456789012")
    assert text == "Sending alert email to [EMAIL] about account [NUMBER]"
    assert counts["email"] == 1 and counts["long_number"] == 1


def test_ordinary_ids_are_left_alone():
    line = "Submitted application application_1760228400123_0412 (TID 3307) job 90417 at 2026-10-12T06:00:00+00:00"
    assert redact(line)[0] == line
