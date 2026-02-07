# bi/superset/superset_register_datasets.py
from superset import app, db
from superset.models.core import Database
from superset.connectors.sqla.models import SqlaTable

DATASETS = [
    # schema, table
    ("gold", "fact_trip_detailed"),
    ("gold", "fact_daily_zone_summary"),
    ("gold", "fact_hourly_demand"),
    ("gold", "fact_revenue_decomposition"),
    ("gold", "v_fact_trip_borough_only"),  # governed view
]

DB_NAME = "Trino - Lakehouse"

def register_dataset(database, schema, table):
    existing = (
        db.session.query(SqlaTable)
        .filter_by(database_id=database.id, schema=schema, table_name=table)
        .one_or_none()
    )
    if existing:
        print(f"Dataset already exists: {schema}.{table}")
        return
    ds = SqlaTable(
        database=database,
        schema=schema,
        table_name=table,
        is_sqllab_view=False,
        fetch_values_predicate=None,
    )
    db.session.add(ds)
    db.session.commit()
    print(f"Registered dataset: {schema}.{table}")

with app.app_context():
    database = db.session.query(Database).filter_by(database_name=DB_NAME).one()
    for schema, table in DATASETS:
        register_dataset(database, schema, table)
