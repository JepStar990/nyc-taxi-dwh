# infra/docker/images/superset/Dockerfile
FROM apache/superset:3.0.0

USER root
# Install Trino SQLAlchemy dialect + client library
RUN pip install --no-cache-dir "trino[sqlalchemy]==0.330.0" "sqlalchemy-trino==0.5.0"

USER superset
WORKDIR /app
