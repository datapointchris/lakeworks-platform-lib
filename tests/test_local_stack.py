"""The `local` target, run against a real Iceberg REST catalog and real object storage.

`catalog_config(Target.LOCAL, ...)` is already covered by the unit tests, which prove it returns the
settings it is meant to return. What they cannot reach is whether those settings build a session
Iceberg accepts, and that is the difference between configuration that looks right and a job that
runs.

Every identifier here is the one a job would use in AWS. The catalog is the default, so a table is
named by its database and table alone and the deployment target never appears in job code.
"""

import os
import time

import httpx2
import pytest

from lakeworks import iceberg
from lakeworks import spark

pytestmark = pytest.mark.local_stack

DATABASE = 'lakeworks_dev_animal_bronze'
TABLE = f'{DATABASE}.shelter_feed'

ROWS = [
    ('a-1', 'intake'),
    ('a-2', 'intake'),
    ('a-3', 'transfer'),
]
SCHEMA = 'animal_key string, event_type string'

REQUIRED_ENVIRONMENT = ('LAKEWORKS_CATALOG_URI', 'LAKEWORKS_S3_ENDPOINT', 'LAKEWORKS_WAREHOUSE')

RUN_COMMAND = 'cd tests/local-stack && docker compose run --rm spark pytest --run-local-stack'
"""How to run this file. The stack supplies the environment, so nothing here defaults."""

CATALOG_READY_TIMEOUT = 60.0
"""Seconds to wait for the catalog. Long enough for a container that is still starting, short enough
that a stack which is not running fails rather than appearing to hang."""


def catalog_answers(uri):
    """Whether the REST catalog is listening.

    Args:
        uri: Catalog base URI.

    Returns:
        True when the catalog answered at all. An error status is an answer — the question is
        whether something is there, not whether it liked the request, and `httpx2` raises on a
        failed connection rather than on a status.
    """
    try:
        httpx2.get(f'{uri}/v1/config', timeout=2)
    except httpx2.HTTPError:
        return False
    return True


def wait_for_catalog(uri):
    """Block until the catalog answers.

    Args:
        uri: Catalog base URI.
    """
    deadline = time.monotonic() + CATALOG_READY_TIMEOUT
    while time.monotonic() < deadline:
        if catalog_answers(uri):
            return
        time.sleep(1)
    pytest.fail(f'No Iceberg REST catalog at {uri} after {CATALOG_READY_TIMEOUT:.0f}s. Run: {RUN_COMMAND}')


@pytest.fixture(scope='module')
def session():
    """A session built the way a job builds one, against the running stack.

    Module-scoped because starting Spark costs seconds and nothing here mutates the table after it
    is written.

    Yields:
        The active session.
    """
    missing = [name for name in REQUIRED_ENVIRONMENT if name not in os.environ]
    if missing:
        pytest.fail(f'{", ".join(missing)} unset, so there is no stack to reach. Run: {RUN_COMMAND}')

    if spark.resolve_target() is not spark.Target.LOCAL:
        target = os.environ['LAKEWORKS_TARGET']
        pytest.fail(f'LAKEWORKS_TARGET is {target!r}, and this file only means anything against `local`. Run: {RUN_COMMAND}')

    wait_for_catalog(os.environ['LAKEWORKS_CATALOG_URI'])

    active = spark.session('local-stack-check')
    try:
        active.sql(f'CREATE NAMESPACE IF NOT EXISTS {DATABASE}')
        yield active
    finally:
        active.sql(f'DROP TABLE IF EXISTS {TABLE} PURGE')
        active.sql(f'DROP NAMESPACE IF EXISTS {DATABASE}')
        active.stop()


@pytest.fixture(scope='module')
def table(session):
    """The table, created through the catalog and written to once.

    Args:
        session: The active session.

    Returns:
        The table identifier, in the form job code uses.
    """
    session.createDataFrame(ROWS, SCHEMA).writeTo(TABLE).using('iceberg').createOrReplace()
    return TABLE


@pytest.fixture
def wap_table(session):
    """A one-row table for a write-audit-publish case, dropped afterwards.

    Function-scoped, unlike `table`. Write-audit-publish mutates the table and a failing audit
    deliberately leaves its branch in place, so a shared table would carry one case's wreckage into
    the next.

    Args:
        session: The active session.

    Yields:
        The table identifier.
    """
    name = f'{DATABASE}.wap_subject'
    session.sql(f'DROP TABLE IF EXISTS {name} PURGE')
    session.createDataFrame([ROWS[0]], SCHEMA).writeTo(name).using('iceberg').createOrReplace()
    yield name
    session.sql(f'DROP TABLE IF EXISTS {name} PURGE')


def test_a_failing_audit_leaves_main_untouched(session, wap_table):
    """The data-safety claim the module leads on, and it cannot be checked without Iceberg.

    Iceberg redirects a write to `spark.wap.branch` only on a table carrying `write.wap.enabled`.
    Without that property the write lands on `main`, the audit reads a branch that never received
    the rows and passes, and the bad rows are published while the caller is told nothing.
    """
    before = sorted((row.animal_key, row.event_type) for row in session.table(wap_table).collect())
    duplicate = session.createDataFrame([ROWS[0]], SCHEMA)

    with (
        pytest.raises(iceberg.AuditFailed),
        iceberg.write_audit_publish(session, wap_table, [iceberg.grain_is_unique('animal_key')]),
    ):
        duplicate.writeTo(wap_table).append()

    after = sorted((row.animal_key, row.event_type) for row in session.table(wap_table).collect())
    assert after == before


def test_a_clean_audit_publishes_and_drops_the_branch(session, wap_table):
    """A passing audit fast-forwards `main` and leaves nothing behind to inspect."""
    with iceberg.write_audit_publish(session, wap_table, [iceberg.rows_arrived()]):
        session.createDataFrame([ROWS[2]], SCHEMA).writeTo(wap_table).append()

    keys = sorted(row.animal_key for row in session.table(wap_table).collect())
    refs = [row.name for row in session.sql(f'SELECT name FROM {wap_table}.refs').collect()]

    assert keys == sorted([ROWS[0][0], ROWS[2][0]])
    assert refs == ['main']


def test_a_hyphenated_branch_name_does_not_break_the_audit(session, wap_table):
    """The default run id carries hyphens, and so does a Step Functions execution name.

    The branch segment is interpolated into a table identifier. Unquoted, a hyphen there parses as
    subtraction and the audit dies on an invalid identifier rather than returning a verdict.
    """
    with iceberg.write_audit_publish(session, wap_table, [iceberg.rows_arrived()], branch='has-hyphens-in-it'):
        session.createDataFrame([ROWS[1]], SCHEMA).writeTo(wap_table).append()

    assert session.table(wap_table).count() == 2


def test_the_run_id_reaches_the_snapshot_summary(session, wap_table):
    """A row is traceable to its run only if the snapshot that committed it carries the id.

    Iceberg builds a snapshot's extra summary entries from write options prefixed
    `snapshot-property.` and from nowhere else, so a run id recorded as a table property cannot be
    read back from any snapshot.
    """
    session.createDataFrame([ROWS[2]], SCHEMA).writeTo(wap_table).options(**iceberg.run_id_options()).append()

    query = f'SELECT summary FROM {wap_table}.snapshots ORDER BY committed_at'
    summaries = [row.summary for row in session.sql(query).collect()]

    assert summaries[-1][iceberg.SNAPSHOT_RUN_ID_PROPERTY] == spark.run_id()


def test_rows_written_through_the_catalog_read_back(session, table):
    """The exit criterion: a job creates an Iceberg table, writes it, and reads it back."""
    read_back = session.table(table)

    assert read_back.count() == len(ROWS)
    assert sorted(row.animal_key for row in read_back.collect()) == sorted(key for key, _ in ROWS)


def test_the_identifier_needs_no_catalog_prefix(session, table):
    """Job code names a database and a table. Which catalog resolves them is configuration.

    This is the claim the whole session factory rests on, and it is the one that cannot be checked
    without a catalog that really resolves the name.
    """
    bare = session.table(table).collect()
    prefixed = session.table(f'{spark.CATALOG_NAME}.{table}').collect()

    assert sorted(bare) == sorted(prefixed)


def test_the_data_files_land_in_object_storage_under_the_table(session, table):
    """The rows are in object storage, not in a local directory Spark chose on its own.

    Asserted against the location the catalog reports for the table, because that is the only
    authority on it. With a REST catalog the server's own warehouse setting places the table, and
    `spark.sql.catalog.lakeworks.warehouse` on the client is inert — it is load-bearing for the Glue
    and EMR branches, where the catalog does not decide placement.

    So this pins that the rows reached S3-compatible storage and sit beneath their own table. It
    does not pin the client warehouse setting, and comparing against `LAKEWORKS_WAREHOUSE` would
    only compare two literals from the same compose file.
    """
    described = session.sql(f'DESCRIBE TABLE EXTENDED {table}').collect()
    location = next(row[1] for row in described if row[0] == 'Location')
    paths = [row.file_path for row in session.sql(f'SELECT file_path FROM {table}.files').collect()]

    assert location.startswith('s3://')
    assert paths
    assert all(path.startswith(location) for path in paths)
