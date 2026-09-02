# CLAUDE.md

Guidance for Claude Code working in this repository.

Read the README first. It carries the three deployment targets, why write-audit-publish is the
default, why assertions return rows, and what the two test markers select.

## Job code must never learn where it runs

`spark.session()` takes the target from `LAKEWORKS_TARGET` and the caller passes a job name and
nothing else. Do not add a parameter, a branch or a helper that lets job code ask which target it
is on. A job that branches on its deployment target is one whose local and cloud behaviour drift
apart silently, and the drift is only visible in production.

The same table identifier has to resolve on all three targets. If a change makes an identifier
work on one and not another, the change is wrong rather than the caller.

## Nothing falls back to a default

A missing `LAKEWORKS_WAREHOUSE` raises. Do not add a default, an `or ''`, or a "sensible" fallback
to make a test or a local run easier. A session pointed at the wrong warehouse writes real data to
the wrong place, and the caller cannot tell that from success.

## Versions are pinned to Glue 5.0

Spark 3.5.4, Python 3.11, Java 17. A local Spark 4.x accepts different Iceberg procedure syntax, so
a job that passes locally then fails in Glue, and the failure reads as an Iceberg bug rather than a
version mismatch. Do not relax the pin to resolve a dependency conflict — resolve the conflict.

## Provenance is set per write

Iceberg builds a snapshot's extra summary entries from write options prefixed
`snapshot-property.`, and from nowhere else. Setting a table property instead records the last run
to touch the table and says nothing about any row in it. Use `run_id_options()` on the write.

This cannot be repaired after the fact, because the snapshots are already committed.

## Assertions return rows, not booleans

Every assertion is SQL that must return zero rows, which is what lets a failure carry the offending
rows as evidence. A new assertion returning a boolean throws away the only useful part of a
failure. Follow the existing builders in `iceberg`.

`no_overlapping_validity` is the one worth understanding before touching: its failure mode raises
nowhere, it silently doubles rows through a join, and it surfaces weeks later as a number somebody
does not trust.

## Tests

```bash
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest
```

The default run needs nothing but this repo. The pure functions — `catalog_config` and the
assertion builders — take arguments and return settings or SQL, so the branches that matter need no
Spark session and no AWS account. Keep new logic on that side of the line where it can be.

`tests/local-stack` runs the suite against MinIO and an Iceberg REST catalog:

```bash
cd tests/local-stack && docker compose run --rm spark pytest --run-local-stack
```

**The markers are deselected, not skipped, and that is deliberate.** A skip is what a test reports
after starting and finding what it needs is absent, and a suite green because everything skipped
reads exactly like one where everything ran. Do not convert a deselect into a skip, and do not add
a guard that makes the local-stack job pass when Docker is missing — that job must go red and say
so.
