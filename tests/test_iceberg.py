"""Tests for the Iceberg audit assertions.

The assertion builders are pure — they return SQL, they do not run it — so the shape of every
generated query is checked here without a Spark session. What cannot be checked without a warehouse
is whether the SQL is *correct against Iceberg*, and that is what `test_local_stack.py` is for.
"""

import datetime as dt
import re
from unittest import mock

from pyspark.sql import SparkSession

from lakeworks import iceberg


def test_maintain_removes_orphans_last_and_only_a_week_behind():
    """Compaction orphans the files it replaces, so orphan removal has to come after it.

    The local lakehouse cannot run orphan removal, so the order and the cutoff are pinned here from
    the statements `maintain` hands a session that records them without running them. A cutoff
    closer than a week can delete files a concurrent writer has staged but not yet committed.
    """
    session = mock.create_autospec(SparkSession, instance=True)
    iceberg.maintain(session, 'db.tbl', retain_snapshots=3)
    statements = [call.args[0] for call in session.sql.call_args_list]

    assert re.findall(r'CALL system\.(\w+)', ' '.join(statements)) == [
        'rewrite_data_files',
        'expire_snapshots',
        'remove_orphan_files',
    ]
    assert all("table => 'db.tbl'" in statement for statement in statements)
    assert 'retain_last => 3' in statements[1]

    (cutoff,) = re.findall(r"older_than => TIMESTAMP '([^']+)'", statements[2])
    week_ago = dt.datetime.now(dt.UTC).replace(tzinfo=None) - dt.timedelta(days=7)
    assert dt.datetime.fromisoformat(cutoff) <= week_ago


def test_grain_assertion_names_every_key_column():
    assertion = iceberg.grain_is_unique('animal_key', 'event_at', 'event_type')

    assert 'animal_key, event_at, event_type' in assertion.sql
    assert 'GROUP BY' in assertion.sql
    assert 'HAVING count(*) > 1' in assertion.sql


def test_every_assertion_substitutes_the_table_placeholder():
    """A `{table}` left unsubstituted would run against a table literally named `{table}`, which
    fails as a missing-table error rather than as a bad assertion."""
    assertions = [
        iceberg.grain_is_unique('k'),
        iceberg.no_overlapping_validity('k'),
        iceberg.rows_arrived(),
    ]
    for assertion in assertions:
        rendered = assertion.sql.format(table='db.tbl')
        assert '{table}' not in rendered
        assert 'db.tbl' in rendered


def test_overlapping_validity_compares_a_table_to_itself():
    assertion = iceberg.no_overlapping_validity('animal_id')
    rendered = assertion.sql.format(table='db.dim_animal')

    assert rendered.count('db.dim_animal') == 2
    assert 'valid_to IS NULL' in rendered


def test_every_assertion_carries_a_description():
    """The description is what reaches whoever reads the alert, so an empty one is a defect."""
    for assertion in (
        iceberg.grain_is_unique('k'),
        iceberg.no_overlapping_validity('k'),
        iceberg.rows_arrived(),
    ):
        assert assertion.description
        assert assertion.name
