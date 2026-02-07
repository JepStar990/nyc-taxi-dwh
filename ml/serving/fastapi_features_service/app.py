from fastapi import FastAPI, HTTPException
from feast import FeatureStore
import os

app = FastAPI(title="NYC Taxi Feature Service")

FEAST_REPO = os.getenv("FEAST_REPO","/workspace/ml/features/feast_repo")
store = FeatureStore(repo_path=FEAST_REPO)

@app.get("/health")
def health():
    return {"status":"ok"}

@app.get("/features/demand")
def features_demand(zone_key: int, pickup_datetime_key: int):
    """
    Returns a small set of online features for (zone_key, pickup_datetime_key).
    Requires Feast materialization to have run.
    """
    try:
        entity_rows = [{"zone_key": zone_key, "pickup_datetime_key": pickup_datetime_key}]
        feats = store.get_online_features(
            features=[
                "demand_features:avg_speed",
                "demand_features:avg_tip_pct",
                "demand_features:has_major_event"
            ],
            entity_rows=entity_rows
        ).to_dict()
        return feats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
