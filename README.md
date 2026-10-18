# lakeworks-platform-lib

The lakeworks platform's shared library. It builds the Spark session a job runs in, and it publishes
a job's Iceberg writes only once they pass an audit.

Every domain pipeline imports it. It imports nothing from any domain. Nothing in it names a domain
or special-cases a tenant. A domain that needs something missing here gets a general capability
added to this library, or solves it in its own code.

`spark` builds the session for each deployment target and reads the run id. `iceberg` holds
write-audit-publish, the audit assertions, run-id stamping and table maintenance.

## Install

```bash
pip install 'lakeworks @ git+https://github.com/datapointchris/lakeworks-platform-lib@<tag>'
git ls-remote --tags https://github.com/datapointchris/lakeworks-platform-lib   # the tags to pin
```

Pin a tag or a commit, never a branch. A pipeline that floats on `main` gets a different library on
every deploy. The resulting failure looks like a data problem.

`pyspark` is not a dependency, because Glue and EMR provide it. Anywhere else, install pyspark
beside it, at the version the `dev` extra in `pyproject.toml` pins. Outside Glue and EMR, Spark
also needs the Iceberg jars `iceberg-spark-runtime-3.5_2.12` and `iceberg-aws-bundle` on its
classpath. `tests/local-stack/Dockerfile` puts both in Spark's own jars directory, at the version
it pins.

## Starting from scratch

Git, uv and Docker are the only prerequisites. Nothing here reaches AWS, and no credentials are
needed.

```bash
git clone https://github.com/datapointchris/lakeworks-platform-lib
cd lakeworks-platform-lib
uv venv && uv pip install -e '.[dev]'   # pyspark at the Glue pin, plus the test tools
.venv/bin/python -m pytest              # the pure tests; anything needing the stack or AWS is deselected
cd tests/local-stack
docker compose run --rm spark pytest --run-local-stack   # build the image, start the stack, test on it
```

The last line is the proof. It builds a session the way a job does. It then writes, audits and
publishes through a real Iceberg REST catalog into S3-compatible storage. A pass means the `local`
target's settings build a session Iceberg accepts, which no pure test can show.

The first run builds the Spark image, which downloads the Iceberg jars from Maven Central. `run`
then starts the stack before running the suite. The stack keeps its data in named volumes between
runs. `docker compose down -v` removes the containers and the volumes.

The keys in `compose.yaml` are not secrets. RustFS is created with them on first start, and nothing
outside the stack can reach it.

## A first job names its tables and nothing else

```python
from lakeworks import iceberg
from lakeworks import spark

session = spark.session('animal-conform')  # the pipeline slug, shown in the Spark UI
events = session.table('lakeworks_dev_animal_bronze.shelter_feed')  # no catalog prefix

table = 'lakeworks_dev_animal_silver.animal_event'  # must already exist
assertions = [
    iceberg.grain_is_unique('animal_key', 'event_at', 'event_type'),
    iceberg.rows_arrived(),
]
with iceberg.write_audit_publish(session, table, assertions):
    events.writeTo(table).options(**iceberg.run_id_options()).append()
```

The write inside the block lands on a staging branch. `main` moves only once every assertion
passes. The write options stamp the run id onto the snapshot the write commits.

These settings decide everything the code does not name:

| Setting | Read by | When unset |
| --- | --- | --- |
| `LAKEWORKS_TARGET` | `spark.session` | `local` |
| `LAKEWORKS_WAREHOUSE` | `spark.session` | raises `KeyError` |
| `LAKEWORKS_S3_ENDPOINT` | `spark.session`, on `local` only | raises `KeyError` |
| `LAKEWORKS_CATALOG_URI` | `spark.session`, on `local` only | raises `KeyError` |
| `LAKEWORKS_RUN_ID` | `spark.run_id` | `local-unorchestrated` |

A `LAKEWORKS_TARGET` that names no target raises `ValueError`, and the message lists the valid
ones.

`LAKEWORKS_TARGET`, `LAKEWORKS_WAREHOUSE` and `LAKEWORKS_RUN_ID` are read from a job argument,
`--LAKEWORKS_TARGET glue`, before the environment. Glue hands a job its arguments in `sys.argv` and
sets no environment variable from them, so a Glue job's Terraform passes these as default
arguments. A job argument wins where both are set. The two endpoint settings are local-only and are
read from the environment.

## `spark` — one codebase on every deployment target, and job code never learns which

`LAKEWORKS_TARGET` selects the catalog configuration. The same table identifier resolves on every
target:

| Target | Catalog | Storage |
| --- | --- | --- |
| `local` | Iceberg REST, at `LAKEWORKS_CATALOG_URI` | S3-compatible, at `LAKEWORKS_S3_ENDPOINT`, path-style |
| `glue` | `GlueCatalog` | S3 |
| `emr` | `GlueCatalog` | S3 |

Every target registers one catalog, `lakeworks`, and makes it the default. A job names a table by
database and table alone. The database name carries the environment, the domain and the layer, so
the catalog name never has to change.

A job passes `session` its name and nothing else. A job that branches on its deployment target is
one whose local and cloud behavior drift apart silently. That drift shows only in production.

**Versions are pinned to AWS Glue 5.0: Spark 3.5.4, Python 3.11, Java 17.** The `dev` extra pins
pyspark exactly. Python has a floor of 3.11, because a higher floor cannot run in Glue. The
local-stack image reproduces all three. A local Spark 4.x accepts different Iceberg procedure
syntax. A job that passes there then fails in Glue, and the failure reads as an Iceberg bug rather
than a version mismatch.

**Nothing that places data falls back to a default.** `LAKEWORKS_WAREHOUSE` has none. A session
pointed at the wrong warehouse writes real data to the wrong place, and the caller cannot tell that
from success. The target does default to `local`. But `local` needs its endpoint and catalog
variables, and neither has a default. A cloud job missing its target therefore raises rather than
starting.

Every target turns on adaptive query execution, with partition coalescing and skew-join handling.

## `iceberg` — write-audit-publish, so a failed audit leaves `main` as it was

A quality check that runs after publication only reports damage. Writing to a branch means a
failing audit leaves the table exactly as it was. No consumer ever observes the bad state. The cost
is one extra state in the state machine.

`write_audit_publish(session, table, assertions)` works in this order:

1. It sets `write.wap.enabled` on the table. Iceberg redirects writes to a branch only on a table
   carrying it. Without it the writes land on `main`, the audit reads a branch that never received
   them and passes, and the bad rows are published.
2. It creates the branch `staged-<run id>`, so concurrent runs cannot collide. `branch=` names it
   instead.
3. It sets `spark.wap.branch`, which redirects the block's writes to the branch. Job code names the
   table it always names, with no branch handling.
4. It yields the branch-qualified identifier, for a block that reads back what it wrote.
5. It runs every assertion against the branch, rather than stopping at the first failure. A run
   that breaks three invariants is diagnosed once.
6. On a clean audit it fast-forwards `main` to the branch, then drops the branch.
7. On a failure it raises `AuditFailed`, keeps the branch, and leaves `main` untouched.

The table must already exist, because the first statement alters it. An exception inside the block
skips the audit. The branch stays, and `main` is untouched.

`AuditFailed` names each failing assertion with its violation count and its description. The rows
themselves stay on the kept branch:

```python
staged = f'{table}.`branch_staged-{spark.run_id()}`'
session.sql(assertions[0].sql.format(table=staged)).show()  # the rows that broke the check
session.sql(f'ALTER TABLE {table} DROP BRANCH `staged-{spark.run_id()}`')  # once read
```

Drop the branch once it has been read. The next run under the same run id reuses it, rejected rows
included. A local run without `LAKEWORKS_RUN_ID` always has the id `local-unorchestrated`, so a
local rerun after a failure audits the old rows again.

### Assertions return the rows that break them, never a boolean

Every assertion is SQL returning the rows that violate it, so zero rows is a pass. A boolean
carries only a verdict. A query that returns rows also says which ones.

| Builder | Fails when |
| --- | --- |
| `grain_is_unique(*cols)` | a key at the declared grain appears more than once, as a bad merge produces |
| `no_overlapping_validity(col)` | an SCD2 key has two rows valid at once |
| `rows_arrived()` | the branch holds no rows at all |

`no_overlapping_validity` reads the columns `valid_from` and `valid_to`, where a null `valid_to`
marks the current row. It is the one worth understanding. Its failure raises nowhere. It silently
doubles every join against the dimension, and it surfaces weeks later as a number somebody does not
trust.

`rows_arrived` counts the whole branch, which holds every row `main` held plus the write. An empty
append to a populated table passes it. It catches an empty run only on a table the write replaces,
or one that started empty. A pipeline that expects an empty load leaves it out on purpose.

A check of your own is an `Assertion`. `{table}` in its SQL becomes the branch-qualified
identifier:

```python
iceberg.Assertion(
    name='no_future_events',
    sql='SELECT * FROM {table} WHERE event_at > current_timestamp()',
    description='An event is dated in the future, so a source clock or a parse is wrong.',
)
```

### `maintain` — compaction, snapshot expiry and orphan removal, in that order

```python
iceberg.maintain(session, table)
```

Skipping maintenance degrades a lakehouse slowly rather than failing visibly. `retain_snapshots=`
sets the fewest snapshots expiry keeps. That is the time-travel window, and so the rollback window.

Orphan removal runs last, because compaction orphans the files it replaces. It removes only files
well behind the current time. A shorter window can delete files a concurrent writer has staged but
not yet committed.

## Run ids — provenance is set per write, never once per table

`spark.run_id()` reads `LAKEWORKS_RUN_ID`. Whatever orchestrates the job sets it, one id per run.
Unset, it is `local-unorchestrated`. A developer run is
then told apart from an orchestrated one, rather than passing for a production run with a missing
id.

`iceberg.run_id_options()` returns the write options that put the id in the summary of the snapshot
that write commits, under the key `lakeworks.run_id`. Iceberg builds a snapshot's extra summary
entries from write options prefixed `snapshot-property.`, and from nowhere else. A table property
is never copied into a snapshot summary. Setting one records the last run to touch the table and
says nothing about any row in it.

```sql
SELECT committed_at, summary['lakeworks.run_id'] AS run_id
FROM lakeworks_dev_animal_silver.animal_event.snapshots
```

That link makes any row traceable to the run that produced it. It cannot be added later, because
the snapshots are already committed. The key cannot be renamed either. A summary is written once,
so a rename strands the provenance on every snapshot already committed. A pipeline that wants the
id on each row writes it into a column itself, from `spark.run_id()`.

## Tests — the default run needs nothing but this repo

`catalog_config` and the assertion builders are pure. They take arguments and return settings or
SQL. The branches that matter are therefore tested with no Spark session and no AWS account. Keep
new logic on that side of the line where it can be.

Each gated marker is deselected unless its flag is passed:

| Marker | Flag | Needs | Carried by |
| --- | --- | --- | --- |
| `local_stack` | `--run-local-stack` | the local lakehouse below | every test in `tests/test_local_stack.py` |
| `integration` | `--run-integration` | an AWS account | no test |

`integration` is registered, and no test carries it. Passing its flag changes nothing about what
runs. A marker that exists and selects nothing reads, from a green run, exactly like coverage.

**Deselected, never skipped.** A skip is what a test reports once it has started and found what it
needs is absent. A suite green because everything skipped reads exactly like one where everything
ran. For the same reason the CI job that runs the local lakehouse has no guard for a missing
Docker. Without Docker it goes red and says so.

CI runs the pure suite and the local lakehouse as separate workflows, on every push to `main` and
every pull request. The lakehouse builds an image and starts containers. A job that takes minutes
never sits in front of the checks that take seconds.

## The local lakehouse — Glue 5.0 reproduced, with no AWS account

The pure tests cannot reach whether those settings build a session Iceberg accepts. That needs a
catalog and object storage. `tests/local-stack` is both, run by the compose command in
[Starting from scratch](#starting-from-scratch).

| Service | Serving |
| --- | --- |
| RustFS | `s3://lakeworks-local-lake/` on `:9000`, with a console at `http://localhost:9001/rustfs/console/` |
| `storage-init` | creates the bucket once RustFS answers; everything behind it starts against a ready store |
| Iceberg REST catalog | the catalog on `:8181`, kept in SQLite on a volume so tables survive a restart |
| Spark | the Glue 5.0 runtime, built from `tests/local-stack/Dockerfile` |

`docker compose up` starts the stack without Spark. Spark sits in the `job` profile, so only `run`
starts it. The image installs from `pyproject.toml`, so a dependency change arrives on the next
build. The source is bind-mounted rather than installed, so an edit to `src/` is live in the next
run.

**The Iceberg client is pinned to the version Glue 5.0 ships**, for the reason the Spark pin
exists. A client ahead of it accepts procedure syntax and table properties Glue rejects. The REST
server runs one minor version ahead, because Iceberg publishes no image at the client's version.
The client is where the pin has to hold, since its library decides what a job may write.

The warehouse is `s3://` rather than `file://` on purpose. `file://` would exercise a different
Iceberg code path than the one that runs in Glue. `S3FileIO` finds the fixture's keys through the
AWS SDK's default credential chain. That is the same chain that finds the task role in Glue.

On `local`, the REST server's own warehouse setting places each table. `LAKEWORKS_WAREHOUSE` is
still required there, but it places tables only on `glue` and `emr`.

**The stack stops standing in for Glue at orphan removal.** `rewrite_data_files` and
`expire_snapshots` pass here. `remove_orphan_files` raises `UnsupportedFileSystemException`. It
lists the table directory through Hadoop's FileSystem rather than through `S3FileIO`, and the image
carries no Hadoop S3 filesystem. Whether Glue's runtime maps the scheme itself is unmeasured.
