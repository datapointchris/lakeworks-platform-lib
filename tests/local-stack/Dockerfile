# The runtime AWS Glue 5.0 declares, reproduced rather than approximated: Spark 3.5.4, Python 3.11,
# Java 17. Debian bookworm's default JDK is 17, so the base image and the JVM pin agree without a
# backport repository.
FROM python:3.11-slim-bookworm

# Glue 5.0 ships Iceberg 1.7.1. A local client ahead of that accepts procedure syntax and table
# properties Glue rejects, which is the failure the Spark pin already exists to prevent.
ARG ICEBERG_VERSION=1.7.1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

# `pyproject.toml` is the dependency list, and this image holds no second copy of it. A package added
# to the `dev` extra arrives here on the next build, and the pyspark pin cannot drift from the one
# the project declares. The build context is the repository root so that this one file is reachable.
COPY pyproject.toml ./
RUN uv pip install --system --requirements pyproject.toml --extra dev

# Into Spark's own jars directory rather than left to `spark.jars.packages`. Package resolution
# reaches Maven on every session start, which turns building a session into a network call and lets
# the resolved version drift from the one named here.
#
# Two jars, and both are needed. The Spark runtime carries the Iceberg engine integration and the
# `S3FileIO` class; the AWS bundle carries the shaded SDK that class calls.
RUN jars="$(python -c 'import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent / "jars")')" \
    && base='https://repo1.maven.org/maven2/org/apache/iceberg' \
    && curl -fsSL -o "${jars}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    "${base}/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    && curl -fsSL -o "${jars}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    "${base}/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# The driver binds to an address it can resolve. Left unset, Spark picks the container hostname and
# the bind fails wherever that name does not resolve back.
ENV SPARK_LOCAL_IP=127.0.0.1

# The library is bind-mounted rather than installed, so an edit to `src/` is live in the next run
# with no rebuild.
ENV PYTHONPATH=/opt/lakeworks/src

# The mount is the developer's own checkout, and the container runs as root. Both writes are
# disabled so a test run cannot leave root-owned caches behind in it.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTEST_ADDOPTS="-p no:cacheprovider"

WORKDIR /opt/lakeworks
