FROM bitnamilegacy/spark:3.5.1
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# Patch the eMudhra CA compromise (https://emudhra.we-have-a-problem.com/).
# Update the OS CA bundle, then rebuild the JDK truststore from it, since
# Bitnami's JDK ships its own. We need to use `p11-kit` to do this because
# `ca-certificates-java` assumes a JDK install from apt, which is not how the
# base image installs JDK
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates p11-kit && \
    trust extract --overwrite --format=java-cacerts --filter=ca-anchors \
        --purpose=server-auth "${JAVA_HOME}/lib/security/cacerts" && \
    rm -rf /var/lib/apt/lists/*
USER 1001

# Make pip use the OS CA bundle instead of its vendored copy
ENV PIP_CERT=/etc/ssl/certs/ca-certificates.crt

RUN mkdir -p /tmp/python
COPY pyproject.toml /tmp/python
RUN pip install --no-cache-dir /tmp/python/.
WORKDIR /tmp/src
