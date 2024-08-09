FROM bitnami/spark:3.5.1
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

RUN mkdir -p /tmp/python
COPY pyproject.toml /tmp/python
WORKDIR /tmp/python

RUN pip install .
