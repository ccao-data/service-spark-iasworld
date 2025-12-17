FROM bitnamilegacy/spark:3.5.6
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# hadolint ignore=DL3002
USER root
RUN mkdir -p /tmp/python
COPY pyproject.toml /tmp/python
RUN pip install --no-cache-dir /tmp/python/.
WORKDIR /tmp/src
