"""Tests for Spark session construction.

`catalog_config` is pure by design — it takes the target and warehouse and returns settings, with
everything that reads the environment kept in `resolve_target` and `session`. That split is what
makes the branch that actually matters testable without a Spark session or an AWS account, and it
is why these tests run in milliseconds.
"""

import pytest

from lakeworks import spark


def test_resolve_target_defaults_to_local(monkeypatch):
    monkeypatch.delenv('LAKEWORKS_TARGET', raising=False)
    assert spark.resolve_target() is spark.Target.LOCAL


@pytest.mark.parametrize('value', ['local', 'glue', 'emr'])
def test_resolve_target_reads_every_member(monkeypatch, value):
    monkeypatch.setenv('LAKEWORKS_TARGET', value)
    assert spark.resolve_target().value == value


def test_unknown_target_names_the_valid_ones(monkeypatch):
    """A typo must fail loudly and say what was expected.

    Falling back to local would run a production job against a laptop's catalog and report success.
    """
    monkeypatch.setenv('LAKEWORKS_TARGET', 'gluee')
    with pytest.raises(ValueError, match='gluee'):
        spark.resolve_target()


def test_every_target_has_a_catalog_configuration(monkeypatch):
    """The dispatch names every enum member, so a new target cannot fall through silently."""
    monkeypatch.setenv('LAKEWORKS_S3_ENDPOINT', 'http://localhost:9000')
    monkeypatch.setenv('LAKEWORKS_CATALOG_URI', 'http://localhost:8181')
    for target in spark.Target:
        config = spark.catalog_config(target, 's3://warehouse/')
        assert config[f'spark.sql.catalog.{spark.CATALOG_NAME}.warehouse'] == 's3://warehouse/'


def test_glue_and_emr_use_the_glue_catalog():
    for target in (spark.Target.GLUE, spark.Target.EMR):
        config = spark.catalog_config(target, 's3://warehouse/')
        key = f'spark.sql.catalog.{spark.CATALOG_NAME}.catalog-impl'
        assert config[key] == 'org.apache.iceberg.aws.glue.GlueCatalog'


def test_local_uses_the_rest_catalog_and_path_style_access(monkeypatch):
    """MinIO serves one host with bucket names in the path; real S3 does not.

    Without path-style access every local read resolves a bucket-as-subdomain that does not exist,
    and the failure looks like a networking problem rather than a configuration one.
    """
    monkeypatch.setenv('LAKEWORKS_S3_ENDPOINT', 'http://localhost:9000')
    monkeypatch.setenv('LAKEWORKS_CATALOG_URI', 'http://localhost:8181')
    config = spark.catalog_config(spark.Target.LOCAL, 's3://warehouse/')

    assert config[f'spark.sql.catalog.{spark.CATALOG_NAME}.type'] == 'rest'
    assert config[f'spark.sql.catalog.{spark.CATALOG_NAME}.s3.path-style-access'] == 'true'
    assert config['spark.hadoop.fs.s3a.path.style.access'] == 'true'


def test_adaptive_execution_is_on_for_every_target(monkeypatch):
    """AQE handles most skew since Spark 3.0, and the jobs that salt are the ones where it is
    demonstrably not enough. Losing it silently would make a skew fix look necessary when it is
    not."""
    monkeypatch.setenv('LAKEWORKS_S3_ENDPOINT', 'http://localhost:9000')
    monkeypatch.setenv('LAKEWORKS_CATALOG_URI', 'http://localhost:8181')
    for target in spark.Target:
        config = spark.catalog_config(target, 's3://warehouse/')
        assert config['spark.sql.adaptive.enabled'] == 'true'
        assert config['spark.sql.adaptive.skewJoin.enabled'] == 'true'


def test_local_requires_its_endpoint_rather_than_defaulting(monkeypatch):
    """A missing endpoint must raise, not fall back.

    A session silently pointed at the wrong warehouse writes real data to the wrong place, and the
    caller cannot tell that from success.
    """
    monkeypatch.delenv('LAKEWORKS_S3_ENDPOINT', raising=False)
    with pytest.raises(KeyError):
        spark.catalog_config(spark.Target.LOCAL, 's3://warehouse/')


def test_run_id_marks_an_unorchestrated_run(monkeypatch):
    """A developer run must be distinguishable from an orchestrated one, rather than looking like a
    production run with a missing id."""
    monkeypatch.delenv('LAKEWORKS_RUN_ID', raising=False)
    assert spark.run_id() == 'local-unorchestrated'

    monkeypatch.setenv('LAKEWORKS_RUN_ID', 'sfn-abc123')
    assert spark.run_id() == 'sfn-abc123'
