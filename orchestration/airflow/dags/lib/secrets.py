# Small helper to read secrets from Vault using Airflow Hashicorp provider
import os
from airflow.hooks.base import BaseHook
from airflow.providers.hashicorp.secrets.vault import VaultClient

def get_vault_webhook(path="secret/data/airflow/slack", key="webhook"):
    # Requires env VAULT_ADDR and VAULT_TOKEN in the Airflow container (dev setup)
    addr = os.getenv("VAULT_ADDR","http://vault:8200")
    token = os.getenv("VAULT_TOKEN","myroot")
    client = VaultClient(url=addr, token=token)
    resp = client.get_secret(path=path)
    if not resp or "data" not in resp or "data" not in resp["data"]:
        return None
    return resp["data"]["data"].get(key)
