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
import urllib.error
import urllib.request

import pytest

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
        whether something is there, not whether it liked the request.
    """
    try:
        urllib.request.urlopen(f'{uri}/v1/config', timeout=2)
    except urllib.error.HTTPError:
        return True
    except OSError:
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
        pytest.fail(f'LAKEWORKS_TARGET is {os.environ["LAKEWORKS_TARGET"]!r}. This file only means anything against `local`.')

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


def test_the_data_files_land_in_the_configured_warehouse(session, table):
    """The rows are in object storage, not in a local directory Spark chose on its own.

    Without this the first two tests pass against a session whose warehouse setting was ignored,
    because a table that reads back correctly says nothing about where its files went.
    """
    warehouse = os.environ['LAKEWORKS_WAREHOUSE']
    paths = [row.file_path for row in session.sql(f'SELECT file_path FROM {table}.files').collect()]

    assert paths
    assert all(path.startswith(warehouse) for path in paths)
