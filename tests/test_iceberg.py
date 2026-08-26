"""Tests for the Iceberg audit assertions.

The assertion builders are pure — they return SQL, they do not run it — so the shape of every
generated query is checked here without a Spark session. What cannot be checked without a warehouse
is whether the SQL is *correct against Iceberg*, and that is what the integration tests are for.
"""

import dataclasses

import pytest

from lakeworks import iceberg


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


def test_zero_rows_means_pass_is_the_convention():
    """Every assertion returns violating rows, never a boolean.

    That is what lets a failure carry evidence — the offending rows — instead of only a verdict.
    """
    assertion = iceberg.rows_arrived()
    rendered = assertion.sql.format(table='db.tbl')
    assert 'WHERE n = 0' in rendered


def test_overlapping_validity_compares_a_table_to_itself():
    assertion = iceberg.no_overlapping_validity('animal_id')
    rendered = assertion.sql.format(table='db.dim_animal')

    assert rendered.count('db.dim_animal') == 2
    assert 'valid_to IS NULL' in rendered


def test_assertions_are_frozen():
    """An assertion handed to the audit must not be mutated by it."""
    assertion = iceberg.rows_arrived()
    with pytest.raises(dataclasses.FrozenInstanceError):
        assertion.name = 'something-else'


def test_every_assertion_carries_a_description():
    """The description is what reaches whoever reads the alert, so an empty one is a defect."""
    for assertion in (
        iceberg.grain_is_unique('k'),
        iceberg.no_overlapping_validity('k'),
        iceberg.rows_arrived(),
    ):
        assert assertion.description
        assert assertion.name
