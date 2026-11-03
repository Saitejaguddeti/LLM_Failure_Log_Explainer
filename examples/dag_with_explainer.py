"""Example DAG: one task that runs past its execution_timeout, with the explainer as its failure callback.

Install this package on the Airflow workers, for example:
    pip install "git+https://github.com/<your-github-username>/llm-failure-log-explainer"
and set LLM_MODEL plus the provider's API key in the worker environment. Without them the callback still
writes a note, using the built-in failure patterns.
"""

from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task

from log_explainer.airflow_callback import make_failure_callback

explain_on_failure = make_failure_callback()


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"on_failure_callback": explain_on_failure, "retries": 0},
    tags=["example"],
)
def explain_log_demo():
    @task(execution_timeout=timedelta(seconds=10))
    def slow_step():
        import time

        time.sleep(60)  # longer than execution_timeout, so Airflow raises AirflowTaskTimeout

    slow_step()


explain_log_demo()
