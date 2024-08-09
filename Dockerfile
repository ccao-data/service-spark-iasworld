FROM bitnami/spark:3.5.1
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

RUN mkdir -p /tmp/python
COPY pyproject.toml /tmp/python
RUN pip install --no-cache-dir /tmp/python/.

WORKDIR /tmp
