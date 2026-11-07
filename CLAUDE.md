# lakeworks-platform-lib

The README carries the three deployment targets, the settings and what each does when unset, why
write-audit-publish is the default, why assertions return rows, how provenance is recorded, and
what the gated test marker selects. This file holds what to keep in mind at the edit site, and the
README holds the reasons.

## Job code must never learn where it runs

`spark.session()` takes the target from `LAKEWORKS_TARGET` and the caller passes a job name and
nothing else. Do not add a parameter, a branch or a helper that lets job code ask which target it
is on. A job that branches on its deployment target is one whose local and cloud behavior drift
apart silently, and the drift is only visible in production.

The same table identifier has to resolve on all three targets. If a change makes an identifier
work on one and not another, the change is wrong rather than the caller.

## The library reaches outside the process through two doors

- **Every Spark and Iceberg effect goes through the `SparkSession` the caller passes in**, as
  `spark.sql(...)` and `spark.conf` on it. The library opens no client, connection or file of its
  own. `spark.session()` is the one place a session is built.
- **Every setting is read through `spark._setting`**, which takes a `--NAME value` job argument
  before the environment. Glue hands settings to a job only as job arguments, so a setting read any
  other way never reaches a Glue job.
- **The one sanctioned exception is `catalog_config`'s local branch.** It reads
  `LAKEWORKS_S3_ENDPOINT` and `LAKEWORKS_CATALOG_URI` straight from the environment. Both exist only
  on the `local` target, whose compose stack sets them as environment variables.

## Each of these has its reason in the README

- No setting that places data gets a default. Do not add a fallback for `LAKEWORKS_WAREHOUSE`.
- pyspark stays pinned to Glue 5.0. Resolve a dependency conflict rather than relaxing the pin.
- Provenance is the per-write option `run_id_options()`, never a table property. A snapshot
  committed without it cannot be repaired.
- A new assertion returns the rows that violate it, never a boolean. Follow the builders in
  `iceberg`.
- `no_overlapping_validity` fails without raising, by doubling rows through a join. Read its README
  paragraph before touching it.
- New logic stays pure where it can, so the default test run needs no Spark session and no AWS.
- The gated markers are deselected, never skipped. Do not add a guard that lets the local-stack job
  pass when Docker is missing.

## Tests

`task setup` installs the dev extra and the git hooks, and `task test` runs the pure suite.
`task test:local-stack` runs the suite against RustFS and an Iceberg REST catalog. `task --list`
shows the rest: `lint`, `typecheck` and `format`.
