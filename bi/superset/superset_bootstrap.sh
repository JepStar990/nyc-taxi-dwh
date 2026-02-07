#!/usr/bin/env bash
set -euo pipefail

# ====== Config ======
SUPERSET_USER="${SUPERSET_USER:-admin}"
SUPERSET_PASSWORD="${SUPERSET_PASSWORD:-admin}"
SUPERSET_FIRSTNAME="${SUPERSET_FIRSTNAME:-Admin}"
SUPERSET_LASTNAME="${SUPERSET_LASTNAME:-User}"
SUPERSET_EMAIL="${SUPERSET_EMAIL:-admin@example.com}"

TRINO_URI="${TRINO_URI:-trino://trino@trino:8080/lake}"   # catalog 'lake' (Iceberg)
TRINO_DB_NAME="${TRINO_DB_NAME:-Trino - Lakehouse}"

DASH_DIR="/workspace/bi/superset/dashboards"
DATASETS_PY="/workspace/bi/superset/superset_register_datasets.py"

# ====== Wait for services ======
echo "Waiting for Superset webserver to be reachable..."
for i in {1..60}; do
  nc -z localhost 8088 && break
  sleep 2
done

echo "Waiting for Trino to be reachable..."
for i in {1..60}; do
  nc -z trino 8080 && break
  sleep 2
done

# ====== Initialize Superset ======
echo "Upgrading Superset DB..."
superset db upgrade

echo "Creating admin user if missing..."
superset fab create-admin \
  --username "${SUPERSET_USER}" \
  --firstname "${SUPERSET_FIRSTNAME}" \
  --lastname "${SUPERSET_LASTNAME}" \
  --email "${SUPERSET_EMAIL}" \
  --password "${SUPERSET_PASSWORD}" || true

echo "Initializing roles & permissions..."
superset init

# ====== Create Trino database connection in Superset ======
echo "Registering Trino database '${TRINO_DB_NAME}'..."
python - <<PY
import os
from superset import app, db
from superset.models.core import Database

uri = os.getenv("TRINO_URI", "${TRINO_URI}")
name = os.getenv("TRINO_DB_NAME", "${TRINO_DB_NAME}")

with app.app_context():
    existing = db.session.query(Database).filter_by(database_name=name).one_or_none()
    if existing:
        existing.sqlalchemy_uri = uri
        db.session.commit()
        print(f"Updated database '{name}' with URI")
    else:
        database = Database(
            database_name=name,
            sqlalchemy_uri=uri,
            allow_csv_upload=False,
            expose_in_sqllab=True
        )
        db.session.add(database)
        db.session.commit()
        print(f"Created database '{name}'")
PY

# ====== Register datasets (physical tables/views) ======
echo "Registering datasets on '${TRINO_DB_NAME}'..."
python "${DATASETS_PY}"

# ====== Import dashboards ======
echo "Importing dashboards from ${DASH_DIR} ..."
for f in "${DASH_DIR}"/*.json; do
  if [ -f "$f" ]; then
    echo "Importing $f"
    superset import-dashboards -p "$f" || echo "Import warning for $f (ensure IDs map to registered datasets)"
  fi
done

echo "Superset bootstrap complete."
