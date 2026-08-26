"""Iceberg operations the department depends on, chiefly write-audit-publish.

The reason WAP is the default rather than an advanced option: a quality check that runs after
publication is a check that reports damage. Writing to a branch means a failing audit leaves the
table exactly as it was, and no consumer ever observed the bad state. The cost is one extra state
in the state machine.

Every commit carries the run id in its snapshot summary, which is what makes any row in any table
traceable back to the run that produced it. That link cannot be added retroactively — the snapshots
are already written — so it is set on every write from the first pipeline onward.
"""

import contextlib
import dataclasses
import datetime as dt
import logging
from collections.abc import Iterator

from pyspark.sql import SparkSession

from lakeworks.spark import run_id

log = logging.getLogger(__name__)

SNAPSHOT_RUN_ID_PROPERTY = 'lakeworks.run_id'
"""Snapshot summary key carrying the run id. A snapshot's summary is written once and never
rewritten, so renaming this strands the provenance on every snapshot already committed."""


class AuditFailed(Exception):
    """An audit assertion failed against the staged branch, so nothing was published."""


@dataclasses.dataclass(frozen=True)
class Assertion:
    """One audit check, expressed as SQL that must return zero rows.

    Zero-rows-means-pass rather than a boolean, because a failing check can then return the
    offending rows and the error message carries evidence instead of only a verdict.

    Attributes:
        name: Short identifier, surfaced in the failure message and in quality results.
        sql: Query returning the rows that violate the assertion. `{table}` is substituted with the
            branch-qualified table identifier.
        description: What a violation means, for whoever reads the alert at an unhelpful hour.
    """

    name: str
    sql: str
    description: str


def grain_is_unique(*key_columns: str) -> Assertion:
    """Assert one row per key. The check that catches SCD2 bugs.

    Args:
        *key_columns: Columns that together form the declared grain.

    Returns:
        An assertion returning any key appearing more than once.
    """
    keys = ', '.join(key_columns)
    return Assertion(
        name=f'grain_unique[{keys}]',
        sql=f'SELECT {keys}, count(*) AS n FROM {{table}} GROUP BY {keys} HAVING count(*) > 1',
        description=f'The declared grain ({keys}) is violated — a merge produced duplicate rows.',
    )


def no_overlapping_validity(key_column: str) -> Assertion:
    """Assert that an SCD2 dimension has no key with two simultaneously-valid rows.

    The failure this catches is the one that silently doubles every fact-to-dimension join, which
    presents as inflated metrics rather than as an error.

    Args:
        key_column: The natural key whose validity periods must not overlap.

    Returns:
        An assertion returning offending key pairs.
    """
    return Assertion(
        name=f'no_overlapping_validity[{key_column}]',
        sql=f"""
            SELECT a.{key_column}
            FROM {{table}} a JOIN {{table}} b
              ON a.{key_column} = b.{key_column}
             AND a.valid_from < b.valid_from
             AND (a.valid_to IS NULL OR a.valid_to > b.valid_from)
        """,
        description=f'Two rows for the same {key_column} are valid at once — joins will double-count.',
    )


def rows_arrived() -> Assertion:
    """Assert the write produced something.

    An empty successful run is the failure that looks most like success. Where a genuinely empty
    load is expected, the pipeline omits this assertion deliberately rather than by oversight.

    Returns:
        An assertion that fails when the table is empty.
    """
    return Assertion(
        name='rows_arrived',
        sql='SELECT 1 FROM (SELECT count(*) AS n FROM {table}) WHERE n = 0',
        description='The run committed no rows. Check the source watermark before assuming this is correct.',
    )


def evaluate(spark: SparkSession, table: str, assertions: list[Assertion]) -> list[tuple[Assertion, int]]:
    """Run every assertion and report violations.

    Runs all of them rather than stopping at the first failure, because a run that violates three
    invariants is diagnosed once instead of three times.

    Args:
        spark: Active session.
        table: Branch-qualified table identifier to audit.
        assertions: Checks to evaluate.

    Returns:
        One `(assertion, violation_count)` pair per failing assertion. Empty when all pass.
    """
    failures = []
    for assertion in assertions:
        violations = spark.sql(assertion.sql.format(table=table)).count()
        if violations > 0:
            log.warning(f'audit failed: {assertion.name} table={table} violations={violations}')
            failures.append((assertion, violations))
        else:
            log.info(f'audit passed: {assertion.name} table={table}')
    return failures


@contextlib.contextmanager
def write_audit_publish(
    spark: SparkSession,
    table: str,
    assertions: list[Assertion],
    branch: str | None = None,
) -> Iterator[str]:
    """Write to an Iceberg branch, audit it, and publish only on a clean audit.

    The body writes to the table as normal. Spark's `spark.wap.branch` setting redirects those
    writes to the branch, so the job code contains no branch handling at all.

    Args:
        spark: Active session.
        table: Fully-qualified table identifier.
        assertions: Checks that must pass before the branch is published.
        branch: Branch name. Defaults to one derived from the run id, so concurrent runs cannot
            collide on a shared staging branch.

    Yields:
        The branch-qualified identifier, for a body that needs to read back what it wrote.

    Raises:
        AuditFailed: If any assertion is violated. The branch is left in place for inspection and
            `main` is untouched.
    """
    staging = branch if branch is not None else f'staged-{run_id()}'
    # Back-quoted, because a run id carries hyphens — `local-unorchestrated` by default, and a Step
    # Functions execution name in AWS — and an unquoted hyphen makes this a subtraction.
    branch_qualified = f'{table}.`branch_{staging}`'

    # Iceberg honours `spark.wap.branch` only on a table carrying this property. Without it the
    # writes below land on `main`, the audit reads a branch that never received them and passes, and
    # `fast_forward` then fails on an ancestry error with the bad rows already published. Set here
    # rather than required of the caller, because the failure is silent at the point it matters.
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.wap.enabled' = 'true')")
    spark.sql(f'ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS `{staging}`')
    previous = spark.conf.get('spark.wap.branch', None)
    spark.conf.set('spark.wap.branch', staging)
    log.info(f'wap: writing to branch table={table} branch={staging}')

    try:
        yield branch_qualified

        if failures := evaluate(spark, branch_qualified, assertions):
            detail = '; '.join(f'{a.name}: {n} violations — {a.description}' for a, n in failures)
            raise AuditFailed(
                f'{len(failures)} assertion(s) failed on {table}, branch `{staging}` retained for '
                f'inspection and main is unchanged. {detail}'
            )

        # Reset before publishing: fast_forward is a metadata operation on the table, not a write
        # to the branch, and leaving WAP set makes its behaviour depend on Iceberg version details.
        spark.conf.unset('spark.wap.branch')
        spark.sql(f"CALL system.fast_forward('{table}', 'main', '{staging}')")
        spark.sql(f'ALTER TABLE {table} DROP BRANCH `{staging}`')
        log.info(f'wap: published table={table} branch={staging}')
    finally:
        if previous is None:
            spark.conf.unset('spark.wap.branch')
        else:
            spark.conf.set('spark.wap.branch', previous)


def run_id_options() -> dict[str, str]:
    """Write options that put the run id in the snapshot the write commits.

    Iceberg builds a snapshot's extra summary entries from write options prefixed
    `snapshot-property.`, and from nowhere else. A table property is not copied into a snapshot
    summary, so provenance is set per write rather than once per table.

    Returns:
        Options for `DataFrameWriterV2.options`.
    """
    return {f'snapshot-property.{SNAPSHOT_RUN_ID_PROPERTY}': run_id()}


def maintain(spark: SparkSession, table: str, retain_snapshots: int = 10) -> None:
    """Compact small files, expire old snapshots, and remove orphans.

    Skipping this is how a lakehouse degrades slowly rather than failing visibly. Ordering matters:
    compaction creates new files and orphans the old ones, so orphan removal runs last.

    Args:
        spark: Active session.
        table: Fully-qualified table identifier.
        retain_snapshots: Snapshots to keep. This is the time-travel window and therefore the undo
            window — for a daily table, ten snapshots is ten days of rollback.
    """
    cutoff = (dt.datetime.now(dt.UTC) - dt.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

    spark.sql(f"CALL system.rewrite_data_files(table => '{table}', options => map('min-input-files', '5'))")
    spark.sql(f"CALL system.expire_snapshots(table => '{table}', older_than => TIMESTAMP '{cutoff}', retain_last => {retain_snapshots})")
    # Last, and with a cutoff well behind now: a shorter window can delete files a concurrent
    # writer has staged but not yet committed.
    spark.sql(f"CALL system.remove_orphan_files(table => '{table}', older_than => TIMESTAMP '{cutoff}')")
    log.info(f'maintenance complete table={table} retained_snapshots={retain_snapshots}')
