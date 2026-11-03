"""Known failure signatures for Airflow and Spark logs.

Each signature has a category, a regex, a plain-English explanation, and the fixes that usually apply.
They are used two ways: as hints in the prompt, and on their own when the tool runs without a model.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Signature:
    id: str
    category: str
    title: str  # reads well after "failed because ..."
    pattern: re.Pattern[str]
    explanation: str
    fixes: tuple[str, ...]


@dataclass(frozen=True)
class Match:
    signature: Signature
    line_no: int  # 1-based
    line: str


def _re(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern)


SIGNATURES: list[Signature] = [
    Signature(
        "spark_executor_oom",
        "memory",
        "a Spark executor ran out of memory",
        _re(
            r"Container killed by YARN for exceeding (?:physical |virtual )?memory limits"
            r"|exited with code 137|java\.lang\.OutOfMemoryError: Java heap space"
        ),
        "An executor used more memory than its container allows and was killed. When the same task keeps "
        "failing on different executors while the others finish, one partition is usually far bigger than "
        "the rest (data skew).",
        (
            "Check the join and group-by keys of the failing stage for skew; split or salt the hot key, or turn "
            "on AQE skew-join handling (spark.sql.adaptive.enabled, spark.sql.adaptive.skewJoin.enabled).",
            "Raise spark.sql.shuffle.partitions so each task handles less data.",
            "If memory really is short, raise spark.executor.memoryOverhead or spark.executor.memory.",
        ),
    ),
    Signature(
        "spark_driver_oom",
        "memory",
        "the Spark driver ran out of memory",
        _re(
            r"spark\.driver\.maxResultSize|OutOfMemoryError: GC overhead limit exceeded"
            r"|Total size of serialized results"
        ),
        "The driver ran out of memory, usually because of collect(), toPandas(), or a large broadcast.",
        (
            "Avoid collect() or toPandas() on large results; write them out instead.",
            "Raise spark.driver.memory or spark.driver.maxResultSize only if the result really belongs on the driver.",
        ),
    ),
    Signature(
        "spark_fetch_failed",
        "shuffle",
        "shuffle data was lost",
        _re(r"FetchFailedException|MetadataFetchFailedException|Missing an output location for shuffle"),
        "Shuffle files were lost, usually because the executor that wrote them died (out of memory, a Spot "
        "reclaim, or a lost node).",
        (
            "Find what killed the executor just before this error and fix that first.",
            "If the cluster uses Spot capacity, keep shuffle-heavy stages on On-Demand nodes.",
        ),
    ),
    Signature(
        "spark_unresolved",
        "schema",
        "Spark could not resolve a column, table, or path",
        _re(r"AnalysisException|UNRESOLVED_COLUMN|TABLE_OR_VIEW_NOT_FOUND|cannot resolve|Path does not exist"),
        "Spark could not find a column, table, or path the job refers to. Either the code has a typo or the "
        "upstream data changed, for example a renamed column or a missing partition.",
        (
            "Compare the name in the error with the current schema of the input.",
            "If the upstream schema changed, update the job and add a schema check before the transform.",
        ),
    ),
    Signature(
        "spark_broadcast_timeout",
        "performance",
        "a broadcast join timed out",
        _re(r"Could not execute broadcast in \d+ secs"),
        "Spark tried to broadcast one side of a join and it took too long, usually because that side was "
        "bigger than expected.",
        (
            "Turn off auto-broadcast for this join (spark.sql.autoBroadcastJoinThreshold=-1) or raise "
            "spark.sql.broadcastTimeout.",
        ),
    ),
    Signature(
        "airflow_task_timeout",
        "timeout",
        "the task ran past its execution_timeout",
        _re(r"AirflowTaskTimeout"),
        "The task ran longer than its execution_timeout and Airflow killed it. The work itself may have been "
        "fine, only slower than usual.",
        (
            "Compare how long the underlying query or job took on earlier runs, and check whether today's input "
            "was bigger.",
            "Look at the database or cluster side (query history, warehouse size, queueing) for the run window.",
            "Raise execution_timeout only once you know why the run got slower.",
        ),
    ),
    Signature(
        "airflow_sensor_timeout",
        "upstream",
        "a sensor gave up waiting for its upstream data",
        _re(r"AirflowSensorTimeout|Sensor has timed out"),
        "The file, partition, or task the sensor was waiting for did not show up in time.",
        (
            "Check whether the upstream job ran and where it wrote its output.",
            "Confirm the sensor looks at the right path or run date for this run.",
        ),
    ),
    Signature(
        "killed_externally",
        "infrastructure",
        "the task process was killed from outside",
        _re(r"Detected zombie|killed externally|received SIGTERM|Negsignal\.SIGKILL|SIGKILL|return code -9"),
        "Something outside the task stopped it: the worker ran out of memory, the pod was evicted, or a deploy "
        "restarted the worker.",
        (
            "Check the worker or pod events (OOMKilled, Evicted) for the run window.",
            "Give the task more memory or move heavy processing off the Airflow worker.",
        ),
    ),
    Signature(
        "import_error",
        "environment",
        "a Python package is missing from the worker",
        _re(r"ModuleNotFoundError|ImportError: "),
        "The code imports a package that the worker environment does not have, or has in an incompatible version.",
        (
            "Add the package to the worker image or requirements and redeploy.",
            "Check for version conflicts after the last image or provider upgrade.",
        ),
    ),
    Signature(
        "auth_failed",
        "permissions",
        "the database or API rejected the credentials",
        _re(
            r"password authentication failed|authentication failed for user|Invalid credentials|401 Unauthorized"
            r"|ExpiredToken|security token included in the request is (?:expired|invalid)|InvalidAccessKeyId"
        ),
        "The credentials the job used were rejected. A rotated or expired password, key, or token is the usual "
        "reason when the same job worked before.",
        (
            "Check whether the password, key, or token was rotated or expired, and update the Airflow connection "
            "or secret.",
            "Confirm which connection id or secret the task actually reads.",
        ),
    ),
    Signature(
        "access_denied",
        "permissions",
        "the job was denied access to a resource",
        _re(r"AccessDenied|Access Denied|403 Forbidden|Status Code: 403|insufficient privileges|permission denied for"),
        "The job's role or user was not allowed to read or write something it needed.",
        ("Check the IAM policy or database grants for the path or object named in the error.",),
    ),
    Signature(
        "connection_failed",
        "connectivity",
        "a database or API could not be reached",
        _re(
            r"Connection refused|could not connect to server|Name or service not known"
            r"|Temporary failure in name resolution|timed out connecting|Read timed out|Connection reset by peer"
        ),
        "The job could not reach a database or API it depends on.",
        (
            "Check that the host is up and reachable from the worker (DNS, security groups, VPN).",
            "If it fails only now and then, add retries with backoff.",
        ),
    ),
    Signature(
        "snowflake_sql_error",
        "database",
        "Snowflake rejected the SQL",
        _re(r"SQL compilation error|snowflake\.connector\.errors\.ProgrammingError"),
        "Snowflake could not compile or run the statement, usually because an object or column is missing or "
        "the role cannot see it.",
        ("Check that the object or column named in the error exists and that the role has access to it.",),
    ),
    Signature(
        "missing_input",
        "data",
        "an expected input file or object is missing",
        _re(r"NoSuchKey|NoSuchBucket|FileNotFoundException|No such file or directory"),
        "The job looked for an input that was not there.",
        ("Check that the upstream job finished and wrote to the expected path for this run date.",),
    ),
    Signature(
        "disk_full",
        "infrastructure",
        "a disk ran out of space",
        _re(r"No space left on device"),
        "A disk filled up, often with Spark shuffle or spill files or with temporary files on the worker.",
        ("Clean up or enlarge the disk; for Spark, check the local spill and shuffle directories.",),
    ),
    Signature(
        "data_quality",
        "data",
        "a data quality check failed",
        _re(
            r"(?i)(?:row count|reconciliation|null|uniqueness|freshness) check failed"
            r"|DataQualityError|expectation failed"
        ),
        "A data quality gate stopped the run. The data, not the code, is the likely problem.",
        ("Look at the rows that failed the check before rerunning; a rerun will not fix bad data.",),
    ),
]


def find_matches(lines: list[str]) -> list[Match]:
    """First line that matches each signature, ordered by where it appears in the log."""
    found: dict[str, Match] = {}
    for no, line in enumerate(lines, start=1):
        for sig in SIGNATURES:
            if sig.id not in found and sig.pattern.search(line):
                found[sig.id] = Match(sig, no, line)
    return sorted(found.values(), key=lambda m: m.line_no)


def matches_any(line: str) -> bool:
    return any(sig.pattern.search(line) for sig in SIGNATURES)
