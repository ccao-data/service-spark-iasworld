FROM bitnami/spark:3.5.1
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# Switch to root user so we can install jq and yq for parsing JSON/YAML
USER root
RUN mkdir -p /tmp/python && \
    mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install --no-install-recommends -y wget=1.21.3-1+b2 jq=1.6-2.1 && \
    wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -q -O /usr/bin/yq && \
    chmod +x /usr/bin/yq && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /tmp/python
RUN pip install --no-cache-dir /tmp/python/.

# User corresponds to dedicated shiny-server user on server. Necessary for
# proper write permissions to target directories
USER 1003:0
WORKDIR /tmp
