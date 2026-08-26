"""Spark session construction, so job code never learns where it runs.

A job that branches on its deployment target is a job whose local behaviour and cloud behaviour
drift apart silently. This module is the one place the difference exists: three catalog
configurations that resolve the same table identifier, so `lakeworks_dev_animal_bronze.shelter_feed`
means the same thing against MinIO on a laptop, against Glue in dev, and against Glue in prod.

The Iceberg and Spark versions are pinned to match AWS Glue 5.0 exactly — Spark 3.5.4, Python 3.11,
Java 17. A local Spark 4.x accepts different Iceberg procedure syntax, so a job that works locally
fails in Glue for reasons that present as an Iceberg bug rather than a version mismatch.
"""

import enum
import logging
import os

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

CATALOG_NAME = 'lakeworks'
"""Spark catalog name. Table identifiers are `{CATALOG_NAME}.{database}.{table}`, and the database
segment already carries env, domain and layer, so this stays constant across every target."""

ICEBERG_EXTENSIONS = 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'


class Target(enum.Enum):
    """Where the session runs. Selected by `LAKEWORKS_TARGET`, never inferred."""

    LOCAL = 'local'
    GLUE = 'glue'
    EMR = 'emr'


def resolve_target() -> Target:
    """Read the deployment target from the environment.

    Returns:
        The declared target.

    Raises:
        ValueError: If `LAKEWORKS_TARGET` is set to something that is not a Target member.
    """
    raw = os.environ.get('LAKEWORKS_TARGET', Target.LOCAL.value)
    try:
        return Target(raw)
    except ValueError as err:
        valid = ', '.join(t.value for t in Target)
        raise ValueError(f'LAKEWORKS_TARGET={raw!r} is not a known target. Expected one of: {valid}') from err


def catalog_config(target: Target, warehouse: str) -> dict[str, str]:
    """Build the Iceberg catalog configuration for a target.

    Pure — takes the target and warehouse and returns settings. Everything that reads the
    environment happens in `resolve_target` and `session`, which keeps the branch that actually
    matters testable without a Spark session or an AWS account.

    Args:
        target: Where the session will run.
        warehouse: Warehouse URI. An `s3://` prefix in every case, including local, because MinIO
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
                # MinIO serves one host with bucket names in the path; real S3 does not.
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
        KeyError: If a required environment variable is absent. Deliberately not defaulted: a
            session pointed at the wrong warehouse writes real data to the wrong place, and the
            caller cannot tell that from success.
    """
    target = resolve_target()
    resolved_warehouse = warehouse if warehouse is not None else os.environ['LAKEWORKS_WAREHOUSE']
    config = catalog_config(target, resolved_warehouse)

    builder = SparkSession.builder.appName(app)
    for key, value in config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    log.info(f'spark session ready: app={app} target={target.value} warehouse={resolved_warehouse}')
    return spark


def run_id() -> str:
    """The run identifier threaded through logs, Iceberg snapshots and lineage.

    Set by the Step Functions execution name in AWS. Falls back to a local marker so a developer
    run is distinguishable from an orchestrated one rather than looking like a production run with
    a missing id.

    Returns:
        The run id.
    """
    return os.environ.get('LAKEWORKS_RUN_ID', 'local-unorchestrated')
