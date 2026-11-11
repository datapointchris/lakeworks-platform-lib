"""Spark session construction, so job code never learns where it runs.

A job that branches on its deployment target is a job whose local behavior and cloud behavior
drift apart silently. This module is the one place the difference exists: three catalog
configurations that resolve the same table identifier, so `lakeworks_dev_animal_bronze.shelter_feed`
means the same thing against RustFS on a laptop, against Glue in dev, and against Glue in prod.

The Iceberg and Spark versions are pinned to match AWS Glue 5.0 exactly: Spark 3.5.4, Python 3.11,
Java 17. The README's `spark` section says why the pin holds.
"""

import enum
import logging
import os
import sys

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

CATALOG_NAME = 'lakeworks'
"""Spark catalog name. Table identifiers are `{CATALOG_NAME}.{database}.{table}`, and the database
segment already carries env, domain and layer, so this stays constant across every target."""

ICEBERG_EXTENSIONS = 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'

LOG_SCHEMA_VERSION = 1
"""Shape of the library's log events, carried on each one as `lakeworks.schema_version`.

Every log record's message is a stable event name, `lakeworks.<event>`, and its values are
attributes under `lakeworks.` passed as `extra=`. A consumer keys on the name and reads the
attributes, so a change to which attributes an event carries raises this number."""


class Target(enum.Enum):
    """Where the session runs. Selected by `LAKEWORKS_TARGET`, never inferred."""

    LOCAL = 'local'
    GLUE = 'glue'
    EMR = 'emr'


def _setting(name: str) -> str | None:
    """Read a setting from a `--NAME value` job argument, or else from the environment.

    Glue hands a job its arguments in `sys.argv` and sets no environment variable from them, so a
    setting passed as a job argument reaches the job only through here. A job argument wins over the
    environment, because it is set for the run.

    Args:
        name: The setting, such as `LAKEWORKS_TARGET`.

    Returns:
        The value, or None when neither source sets it.
    """
    flag = f'--{name}'
    arguments = sys.argv[1:]
    for index, argument in enumerate(arguments):
        if argument == flag and index + 1 < len(arguments):
            return arguments[index + 1]
        if argument.startswith(f'{flag}='):
            return argument.removeprefix(f'{flag}=')
    return os.environ.get(name)


def resolve_target() -> Target:
    """Read the deployment target from `LAKEWORKS_TARGET`.

    Returns:
        The declared target, or LOCAL when nothing sets it.

    Raises:
        ValueError: If `LAKEWORKS_TARGET` is set to something that is not a Target member.
    """
    raw = _setting('LAKEWORKS_TARGET') or Target.LOCAL.value
    try:
        return Target(raw)
    except ValueError as err:
        valid = ', '.join(t.value for t in Target)
        raise ValueError(f'LAKEWORKS_TARGET={raw!r} is not a known target. Expected one of: {valid}') from err


def catalog_config(target: Target, warehouse: str) -> dict[str, str]:
    """Build the Iceberg catalog configuration for a target.

    The Glue and EMR branch reads nothing but its arguments. The local branch also reads
    `LAKEWORKS_S3_ENDPOINT` and `LAKEWORKS_CATALOG_URI` from the environment, and raises KeyError when
    either is unset. Neither branch builds a session, so the result is testable without Spark or AWS.

    Args:
        target: Where the session will run.
        warehouse: Warehouse URI. An `s3://` prefix in every case, including local, because RustFS
            is S3-compatible and using `file://` locally would exercise a different Iceberg code
            path than the one that runs in AWS.

    Returns:
        Spark configuration keys and values.

    Raises:
        ValueError: If a Target member has no branch here.
    """
    common = {
        'spark.sql.extensions': ICEBERG_EXTENSIONS,
        f'spark.sql.catalog.{CATALOG_NAME}': 'org.apache.iceberg.spark.SparkCatalog',
        f'spark.sql.catalog.{CATALOG_NAME}.warehouse': warehouse,
        'spark.sql.defaultCatalog': CATALOG_NAME,
        # AQE handles most skew since Spark 3.0. Left on deliberately, and the jobs that need
        # salting are the ones where it demonstrably is not enough.
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.adaptive.skewJoin.enabled': 'true',
    }

    match target:
        case Target.LOCAL:
            endpoint = os.environ['LAKEWORKS_S3_ENDPOINT']
            return common | {
                f'spark.sql.catalog.{CATALOG_NAME}.type': 'rest',
                f'spark.sql.catalog.{CATALOG_NAME}.uri': os.environ['LAKEWORKS_CATALOG_URI'],
                f'spark.sql.catalog.{CATALOG_NAME}.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
                f'spark.sql.catalog.{CATALOG_NAME}.s3.endpoint': endpoint,
                # RustFS serves one host with bucket names in the path; real S3 does not.
                f'spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access': 'true',
                'spark.hadoop.fs.s3a.endpoint': endpoint,
                'spark.hadoop.fs.s3a.path.style.access': 'true',
            }
        case Target.GLUE | Target.EMR:
            return common | {
                f'spark.sql.catalog.{CATALOG_NAME}.catalog-impl': 'org.apache.iceberg.aws.glue.GlueCatalog',
                f'spark.sql.catalog.{CATALOG_NAME}.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
            }

    raise ValueError(f'No catalog configuration for target {target}')


def session(app: str, warehouse: str | None = None) -> SparkSession:
    """Build the Spark session for this job.

    Args:
        app: Application name. Surfaces in the Spark UI and in EMR/Glue run listings, so it is the
            pipeline slug rather than the module name.
        warehouse: Warehouse URI. Defaults to `LAKEWORKS_WAREHOUSE`.

    Returns:
        A configured session. Idempotent — `getOrCreate` returns the existing session inside Glue,
        which already built one before the job script runs.

    Raises:
        KeyError: If a required setting is absent. Deliberately not defaulted: a session pointed at
            the wrong warehouse writes real data to the wrong place, and the caller cannot tell that
            from success.
    """
    target = resolve_target()
    resolved_warehouse = warehouse if warehouse is not None else _setting('LAKEWORKS_WAREHOUSE')
    if resolved_warehouse is None:
        raise KeyError(
            'LAKEWORKS_WAREHOUSE is not set. Pass the warehouse URI as the job argument '
            '`--LAKEWORKS_WAREHOUSE s3://<bucket>/<prefix>/`, set the environment variable '
            'LAKEWORKS_WAREHOUSE to it, or pass `warehouse=` to spark.session.'
        )
    config = catalog_config(target, resolved_warehouse)

    builder = SparkSession.builder.appName(app)
    for key, value in config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    log.info(
        'lakeworks.spark_session_ready',
        extra={
            'lakeworks.schema_version': LOG_SCHEMA_VERSION,
            'lakeworks.app': app,
            'lakeworks.target': target.value,
            'lakeworks.warehouse': resolved_warehouse,
        },
    )
    return spark


def run_id() -> str:
    """The run identifier, read from `LAKEWORKS_RUN_ID`.

    `iceberg.run_id_options` puts it in the summary of the snapshot a write commits, and
    `iceberg.write_audit_publish` names its default staging branch after it. A pipeline that wants
    it in its own log lines or columns reads it here.

    Whatever orchestrates the job sets it, one id per run. Unset, it is `local-unorchestrated`, so a
    developer run is told apart from an orchestrated one. A cloud run whose orchestrator sets no id
    gets the same marker, and its default staging branch is shared with every other such run.

    Returns:
        The run id.
    """
    return _setting('LAKEWORKS_RUN_ID') or 'local-unorchestrated'


def resolve_table(declared: str) -> str:
    """A table identifier as the pipeline spec declares it, with `{env}` filled from `LAKEWORKS_ENV`.

    The spec's `reads` and `writes` name each table with an `{env}` placeholder, and the job's
    Terraform fills it from the same environment to grant those databases. A job that passes the
    spec's own string here reads the table its grant covers.

    Args:
        declared: The identifier as the spec writes it, such as
            `lakeworks_{env}_animal_bronze.shelter_feed`.

    Returns:
        The identifier in this environment, such as `lakeworks_dev_animal_bronze.shelter_feed`.

    Raises:
        KeyError: If `LAKEWORKS_ENV` is absent. Deliberately not defaulted, because it places data.
        ValueError: If `declared` carries no `{env}` placeholder.
    """
    if '{env}' not in declared:
        raise ValueError(f'{declared!r} carries no {{env}} placeholder. Pass the identifier as the pipeline spec writes it.')
    env = _setting('LAKEWORKS_ENV')
    if env is None:
        raise KeyError(
            'LAKEWORKS_ENV is not set. Pass the environment as the job argument `--LAKEWORKS_ENV <env>`, '
            'or set the environment variable LAKEWORKS_ENV to it.'
        )
    return declared.replace('{env}', env)
