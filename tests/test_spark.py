"""Tests for Spark session construction.

`catalog_config` returns settings without building a session, so the branch that differs per
target is tested without Spark or an AWS account, in milliseconds.
"""

import ast
import pathlib
import sys

import pytest

import lakeworks
from lakeworks import spark


def test_every_setting_is_read_through_one_function():
    """A setting read straight from the environment never reaches a Glue job.

    Glue hands a job its settings only as job arguments, and `_setting` reads those before the
    environment. So it is the one function that may touch `os.environ` or `os.getenv`.
    """
    package = pathlib.Path(lakeworks.__file__).parent
    bypasses = []
    for path in sorted(package.rglob('*.py')):
        tree = ast.parse(path.read_text(), filename=str(path))
        allowed = {
            id(node)
            for function in ast.walk(tree)
            if isinstance(function, ast.FunctionDef) and function.name == '_setting'
            for node in ast.walk(function)
        }
        bypasses += [
            f'{path.relative_to(package.parent)}:{node.lineno}'
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == 'os'
            and node.attr in ('environ', 'getenv')
            and id(node) not in allowed
        ]

    assert bypasses == []


def test_resolve_target_defaults_to_local(monkeypatch):
    monkeypatch.delenv('LAKEWORKS_TARGET', raising=False)
    assert spark.resolve_target() is spark.Target.LOCAL


def test_a_glue_job_argument_sets_the_target_over_the_environment(monkeypatch):
    """Read from the environment alone, a Glue job given `--LAKEWORKS_TARGET glue` resolves to `local`."""
    monkeypatch.setenv('LAKEWORKS_TARGET', 'local')
    monkeypatch.setattr(sys, 'argv', ['conform.py', '--JOB_ID', 'j_1', '--LAKEWORKS_TARGET', 'glue', '--enable-metrics', 'true'])
    assert spark.resolve_target() is spark.Target.GLUE


def test_unknown_target_names_the_valid_ones(monkeypatch):
    """A typo must fail loudly and say what was expected.

    Falling back to local would run a production job against a laptop's catalog and report success.
    """
    monkeypatch.setenv('LAKEWORKS_TARGET', 'gluee')
    with pytest.raises(ValueError, match='gluee'):
        spark.resolve_target()


def test_glue_and_emr_use_the_glue_catalog():
    for target in (spark.Target.GLUE, spark.Target.EMR):
        config = spark.catalog_config(target, 's3://warehouse/')
        key = f'spark.sql.catalog.{spark.CATALOG_NAME}.catalog-impl'
        assert config[key] == 'org.apache.iceberg.aws.glue.GlueCatalog'


def test_local_uses_the_rest_catalog_and_path_style_access(monkeypatch):
    """RustFS serves one host with bucket names in the path; real S3 does not.

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
    """The local target raises KeyError when its S3 endpoint is unset, rather than falling back."""
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


def test_a_spec_table_resolves_in_the_job_arguments_environment(monkeypatch):
    """A job reading an env-less name reads a database its grant does not cover, so an unset env refuses."""
    monkeypatch.setattr(sys, 'argv', ['conform.py', '--LAKEWORKS_ENV', 'prod'])
    assert spark.resolve_table('lakeworks_{env}_animal_bronze.shelter_feed') == 'lakeworks_prod_animal_bronze.shelter_feed'

    monkeypatch.setattr(sys, 'argv', ['conform.py'])
    monkeypatch.delenv('LAKEWORKS_ENV', raising=False)
    with pytest.raises(KeyError, match='LAKEWORKS_ENV'):
        spark.resolve_table('lakeworks_{env}_animal_bronze.shelter_feed')
