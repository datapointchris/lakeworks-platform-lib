# lakeworks-platform-lib

The platform team's shared library. Every domain pipeline imports it; it imports nothing from any
domain.

```bash
pip install 'lakeworks @ git+https://github.com/datapointchris/lakeworks-platform-lib@v0.1.0'
```

Pinned by tag, never by branch. A pipeline that floats on `main` gets a different library on every
deploy, and the resulting failure looks like a data problem.

## What is in it

| Module | Responsibility |
| --- | --- |
| `spark` | Session construction for three deployment targets, and the run id |
| `iceberg` | Write-audit-publish, the audit assertions, run-id stamping, table maintenance |

## `spark` — one codebase, three deployments

Job code never learns where it runs. A job that branches on its deployment target is a job whose
local and cloud behaviour drift apart silently.

```python
from lakeworks import spark

session = spark.session('animal-conform')
events = session.table('lakeworks_dev_animal_bronze.shelter_feed')
```

`LAKEWORKS_TARGET` selects the catalog configuration, and the same table identifier resolves in all
three:

| Target | Catalog | Warehouse |
| --- | --- | --- |
| `local` | Iceberg REST (the Apache fixture, in `tests/local-stack`) | MinIO, path-style access |
| `glue` | `GlueCatalog` | S3 |
| `emr` | `GlueCatalog` | S3 |

**Versions are pinned to AWS Glue 5.0 — Spark 3.5.4, Python 3.11, Java 17.** A local Spark 4.x
accepts different Iceberg procedure syntax, so a job that works locally fails in Glue for reasons
that present as an Iceberg bug rather than a version mismatch.

**Nothing defaults.** A missing `LAKEWORKS_WAREHOUSE` raises rather than falling back, because a
session pointed at the wrong warehouse writes real data to the wrong place and the caller cannot
tell that from success.

## `iceberg` — write-audit-publish as the default

A quality check that runs after publication is a check that reports damage. Writing to a branch
means a failing audit leaves the table exactly as it was, and no consumer ever observed the bad
state. It costs one extra state in the state machine.

```python
from lakeworks import iceberg

assertions = [
    iceberg.grain_is_unique('animal_key', 'event_at', 'event_type'),
    iceberg.rows_arrived(),
]

table = 'lakeworks_dev_animal_silver.animal_event'
with iceberg.write_audit_publish(session, table, assertions):
    events.writeTo(table).options(**iceberg.run_id_options()).append()
```

Job code names the table it always names. `spark.wap.branch` redirects the write to the staging
branch, so there is no branch handling in the pipeline at all.

On a clean audit the branch fast-forwards into `main` and is dropped. On a failure it raises
`AuditFailed`, **retains the branch for inspection**, and leaves `main` untouched.

### Assertions return rows, never a boolean

Every assertion is SQL that must return zero rows. That is what lets a failure carry evidence — the
offending rows — instead of only a verdict.

| Builder | Catches |
| --- | --- |
| `grain_is_unique(*cols)` | A merge that produced duplicate rows at the declared grain |
| `no_overlapping_validity(col)` | An SCD2 key valid twice at once, which silently doubles every join |
| `rows_arrived()` | An empty successful run — the failure that looks most like success |

`no_overlapping_validity` is the one worth understanding. Its failure mode does not raise anywhere;
it inflates metrics, and it is usually found weeks later by someone who does not trust a number.

## Run ids

One id from the Step Functions execution reaches every layer — log lines, the Iceberg snapshot
summary, and the output table's `_source_run_id`. `run_id_options()` returns the write options that
put it in the summary of the snapshot that write commits.

**Provenance is set per write, not once per table.** Iceberg builds a snapshot's extra summary
entries from write options prefixed `snapshot-property.`, and from nowhere else. A table property is
never copied into a snapshot summary, so a call that sets one records the last run to touch the
table and tells you nothing about any row in it.

That link is what makes any row traceable back to the run that produced it. It cannot be added
later, because the snapshots are already written.

## Tests

```bash
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest
```

Fast because the design allows it. `catalog_config` and the assertion builders are pure — they take
arguments and return settings or SQL — so the branches that matter are tested with no Spark session
and no AWS account.

Two markers are deselected unless their flag is passed, so the default run needs nothing beyond this
repo.

| Marker | Flag | Needs | Tests carrying it |
| --- | --- | --- | --- |
| `local_stack` | `--run-local-stack` | The local lakehouse below | All of `tests/test_local_stack.py` |
| `integration` | `--run-integration` | An AWS account | None yet |

`integration` is registered and nothing carries it, so passing its flag today changes nothing about
what runs. It is named here because a marker that exists and selects nothing reads, from a green
run, exactly like coverage.

Deselected rather than skipped. A skip is what a test reports once it has started and found what it
needs is absent, and a suite green because everything skipped reads exactly like one where
everything ran.

## The local lakehouse

What the pure tests cannot reach is whether those settings build a session Iceberg accepts. That
needs a catalog and object storage. `tests/local-stack` is both, and one command is the whole
workflow:

```bash
cd tests/local-stack
docker compose run --rm spark pytest --run-local-stack
```

`run` starts everything the job depends on, then runs the suite inside a container pinned to the
Glue 5.0 runtime. `docker compose down -v` removes the stack and everything in it.

| Service | Serving |
| --- | --- |
| MinIO | `s3://lakeworks-local-lake/`, with a console on `:9001` |
| Iceberg REST catalog | The protocol Glue speaks, on `:8181` |
| Spark | Spark 3.5.4, Python 3.11, Java 17 — built from `tests/local-stack/Dockerfile` |

**The Iceberg client is pinned to the version Glue 5.0 ships**, for the reason the Spark pin exists:
a client ahead of it accepts procedure syntax and table properties Glue rejects. The REST server
runs one minor ahead, because Iceberg publishes no image at that version. The client is where the
pin has to hold, since its library decides what a job may write.

Nothing here reaches AWS, and no credentials are needed. The warehouse is `s3://` rather than
`file://` on purpose — `file://` would exercise a different Iceberg code path than the one that runs
in Glue.
