FROM spark:3.5.9-java17-python3
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# Switch to the root user during setup, since the base image defaults to
# the `spark` user
USER root

# Install Python dependencies system-wide, since our Compose file is configured
# to run containers using the host user's UID rather than the image's `spark`
# user. Upgrade pip first, since the version installed by apt is too old to
# read the `[project]` metadata in `pyproject.toml`
COPY pyproject.toml /tmp/python/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir /tmp/python/. && \
    rm -rf /tmp/python

# Give the container's user a name and home directory via nss_wrapper.
#
# Background: Linux tracks users by number (UID). Names and home directories
# are looked up separately, normally in `/etc/passwd`. Our Compose file runs
# containers using the host user's UID so that output files in the mounted
# `target/` and `logs/` directories are owned by that host user. That UID
# doesn't exist in this image's `/etc/passwd`, so lookups like `whoami` fail.
# Spark and Hadoop need a username and home directory, and crash without them.
#
# Since the host UID isn't known when the image is built, we can't simply add
# it to `/etc/passwd` here. Instead, we use `nss_wrapper`, a library that
# intercepts user lookups and answers them from the files named in the env vars
# `NSS_WRAPPER_PASSWD` and `NSS_WRAPPER_GROUP`. At startup, our `entrypoint.sh`
# script adds the container's UID to the passwd file. The Apache Spark, Bitnami
# Spark, and official Postgres images all use `nss_wrapper` for this same
# problem, so we're following a standard pattern.
#
# Here's what each of the following commands does, in order:
#
# 1. Copy the real passwd/group files to a new directory `/opt/nss` so existing
#    users like `root` and `spark` still resolve once lookups are redirected to
#    the copies.
# 2. Make the copies writable by group 0 using `chgrp` and `chmod`. Our services
#    defined in our Compose file run containers as `<host UID>:0`, so whatever
#    the host UID, each process is in group 0 and can add its own entry. The
#    real `/etc/passwd` stays read-only; making it writable instead would let
#    any process add a UID 0 (root) user and switch to it with `su`. That
#    doesn't work with the copies, because programs like `su` that grant
#    privileges ignore the copies and only ever read the real `/etc/passwd`.
# 3. Symlink the library to a fixed path using `ln`, since apt installs it under
#    a directory that varies by CPU architecture
#    (e.g. `/usr/lib/x86_64-linux-gnu`).
RUN mkdir -p /opt/nss && \
    cp /etc/passwd /etc/group /opt/nss/ && \
    chgrp -R 0 /opt/nss && \
    chmod -R g+w /opt/nss && \
    ln -s "$(find /usr/lib -name libnss_wrapper.so -print -quit)" \
        /usr/local/lib/libnss_wrapper.so
COPY --chmod=755 entrypoint.sh /opt/service-entrypoint.sh

# Put Spark's CLI tools (e.g. `spark-submit`) on the PATH. Also move worker
# scratch space out of `$SPARK_HOME/work`, which is owned by the `spark` user
# and so won't be writable by the host UID
ENV PATH="${SPARK_HOME}/bin:${PATH}" \
    SPARK_WORKER_DIR=/tmp/spark-worker

# Turn on `nss_wrapper` for every process in the container. `LD_PRELOAD` tells
# Linux to load the library into each program before it starts, which is how
# it intercepts user lookups (privilege-granting programs like `su` ignore
# this configuration). We set these variables in the image, rather than in
# `entrypoint.sh`, so that processes started later with `docker exec` (which
# skips the entrypoint) also get them. We set these vars after all `RUN`
# steps so that they don't affect the build
ENV LD_PRELOAD=/usr/local/lib/libnss_wrapper.so \
    NSS_WRAPPER_PASSWD=/opt/nss/passwd \
    NSS_WRAPPER_GROUP=/opt/nss/group

# Switch back to the `spark` user, which will act as the default user if none is
# specified. We don't expect this to happen since our Compose file overrides
# this with the host UID
USER spark
WORKDIR /tmp/src

ENTRYPOINT ["/opt/service-entrypoint.sh"]
