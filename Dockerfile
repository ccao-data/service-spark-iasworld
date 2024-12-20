FROM bitnami/spark:3.5.1
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# Switch to root user so we can install jq and yq for parsing JSON/YAML
# hadolint ignore=DL3002
USER root
RUN mkdir -p /tmp/python && \
    mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install --no-install-recommends -y wget=1.21.3-1+b2 jq=1.6-2.1 && \
    wget https://github.com/mikefarah/yq/releases/download/v4.44.3/yq_linux_amd64 -q -O /usr/bin/yq && \
    chmod +x /usr/bin/yq && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /tmp/python
RUN pip install --no-cache-dir /tmp/python/.
WORKDIR /tmp/src
