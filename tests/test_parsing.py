import gzip

from conftest import SAMPLES

from log_explainer.parsing import build_excerpt, read_log


def excerpt(name):
    return build_excerpt(read_log(SAMPLES / name), name)


def test_airflow_metadata():
    ex = excerpt("airflow_task_timeout.log")
    assert ex.source == "airflow"
    assert ex.metadata["dag_id"] == "daily_trades_load"
    assert ex.metadata["task_id"] == "merge_trades"
    assert ex.metadata["run_id"] == "scheduled__2026-10-11T06:00:00+00:00"
    assert (ex.metadata["try_number"], ex.metadata["max_tries"]) == ("2", "3")
    assert ex.metadata["operator"] == "SQLExecuteQueryOperator"
    assert ex.label == "daily_trades_load.merge_trades (try 2)"


def test_exception_is_the_line_after_the_traceback():
    ex = excerpt("airflow_task_timeout.log")
    assert ex.exception == "airflow.exceptions.AirflowTaskTimeout: Timeout, PID: 48211"


def test_traceback_frames_are_not_listed_as_error_lines():
    text = excerpt("airflow_task_timeout.log").text
    errors = text.split("## Error lines")[1].split("## ")[0]
    assert "raise AirflowTaskTimeout" not in errors


def test_spark_metadata_and_failed_stage():
    ex = excerpt("spark_executor_oom.log")
    assert ex.source == "spark"
    assert ex.metadata["app_name"] == "trades_daily_aggregates"
    assert ex.metadata["app_id"] == "application_1760228400123_0412"
    assert ex.metadata["failed_stage"] == "stage 14.0, task 112 failed 4 times"
    assert ex.label == "Spark app trades_daily_aggregates"


def test_repeated_errors_are_collapsed():
    text = excerpt("spark_executor_oom.log").text
    assert "Lost executor 7" in text
    assert "Lost executor 9" not in text  # same message on another executor
    assert "[same message 4 times]" in text


def test_spark_settings_and_what_happened_before_the_error():
    text = excerpt("spark_executor_oom.log").text
    assert "spark.executor.memoryOverhead=2g" in text
    assert "Starting task 112.0 in stage 14.0" in text  # the task that kept dying...
    assert "(199/200)" in text  # ...while the other 199 finished


def test_py4j_error_keeps_the_java_exception_under_it():
    first, second = excerpt("spark_executor_oom.log").exception.splitlines()[:2]
    assert first.startswith("py4j.protocol.Py4JJavaError")
    assert second.startswith(": org.apache.spark.SparkException: Job aborted due to stage failure")


def test_pyspark_analysis_exception_keeps_the_plan():
    ex = excerpt("spark_analysis_exception.log")
    assert "UNRESOLVED_COLUMN" in ex.exception and "`trade_date`" in ex.exception
    assert "'Filter ('trade_dt = 2026-10-12)" in ex.exception
    assert ex.text.count('trades.filter(F.col("trade_dt") == RUN_DATE)') == 1  # traceback shown once


def test_caused_by_chain_gives_the_root_cause():
    log = "\n".join(
        [
            "26/10/12 03:00:00 ERROR Executor: Exception in task 3.0 in stage 2.0 (TID 41)",
            "org.apache.spark.SparkException: Task failed while writing rows.",
            "\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:432)",
            "Caused by: java.io.IOException: Error writing to s3://acme-datalake-curated/positions/",
            "\tat org.apache.hadoop.fs.s3a.S3AOutputStream.close(S3AOutputStream.java:120)",
            "Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Service: Amazon S3; "
            "Status Code: 403)",
            "\t... 12 more",
        ]
    )
    ex = build_excerpt(log, "executor.log")
    assert ex.exception.startswith("org.apache.spark.SparkException: Task failed while writing rows.")
    assert ex.root_cause.startswith("Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied")
    assert [m.signature.id for m in ex.matches] == ["access_denied"]


def test_gzipped_logs_are_read(tmp_path):
    gz = tmp_path / "stderr.gz"
    gz.write_bytes(gzip.compress((SAMPLES / "spark_analysis_exception.log").read_bytes()))
    assert read_log(gz) == read_log(SAMPLES / "spark_analysis_exception.log")


def test_excerpt_respects_the_size_cap():
    big = (SAMPLES / "spark_executor_oom.log").read_text() * 40
    assert len(build_excerpt(big, "big.log", max_chars=4000).text) <= 4000


def test_unknown_logs_still_get_an_excerpt():
    ex = build_excerpt("starting export\nERROR - disk quota exceeded on /data/exports", "export.log")
    assert ex.source == "unknown"
    assert ex.label == "export.log"
    assert "disk quota exceeded" in ex.text
