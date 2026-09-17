"""Custom login + persona -> service-principal auth.

End users have NO Databricks identity. They log in against a small local users
table (seeded in Lakebase). Each user maps to a persona ("A" or "B"), and each
persona maps to a Databricks service principal whose OAuth (M2M) credentials live
in the secret scope (${var.secret_scope}). All downstream Databricks calls (Genie,
SQL Statement Execution for the UC-function tools) run as that persona's service
principal, so Unity Catalog enforces the right data access per persona.

NOTE: personas are ORTHOGONAL to the customer catalog (A/B/C). The DAB deploys
this one app image into a customer catalog; personas are the in-app RBAC demo
(analyst vs manager) that shows UC enforcing access on identical code.

NOTE (productionization): running as a *different* SP per request is not a
first-class Databricks Apps feature; we hold multiple SP creds in a secret scope
and mint M2M tokens per persona. Tokens are cached until shortly before expiry.
"""
from __future__ import annotations

import os
import time
import threading
from dataclasses import dataclass

import requests
from databricks.sdk import WorkspaceClient

# --- config (endpoint IDs are non-secret; creds come from the secret scope) ---
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
# SECRET_SCOPE is mapped from ${var.secret_scope} by app.yaml; keep the legacy
# CEREBRO_SECRET_SCOPE name as a fallback for the reference app's env.
SECRET_SCOPE = os.environ.get("SECRET_SCOPE") or os.environ.get("CEREBRO_SECRET_SCOPE", "cerebro_demo")

# persona -> secret-scope keys holding that SP's client_id / client_secret
_PERSONA_SECRET_KEYS = {
    "A": ("persona_a_client_id", "persona_a_client_secret"),
    "B": ("persona_b_client_id", "persona_b_client_secret"),
}

PERSONA_LABELS = {
    "A": "Analyst (less privileged)",
    "B": "Manager (full access)",
}


@dataclass
class Persona:
    key: str            # "A" | "B"
    label: str          # human label
    client_id: str      # SP application id
    _secret: str        # SP oauth secret


# The app itself runs as its own principal; we use it only to read the secret
# scope that holds the persona SP credentials.
_app_client = WorkspaceClient()

_persona_cache: dict[str, Persona] = {}
_token_cache: dict[str, tuple[str, float]] = {}   # persona_key -> (token, expiry_epoch)
_lock = threading.Lock()


def _read_secret(key: str) -> str:
    """Read a value from the Databricks secret scope as the app principal."""
    resp = _app_client.secrets.get_secret(scope=SECRET_SCOPE, key=key)
    import base64
    return base64.b64decode(resp.value).decode("utf-8")


def get_app_token() -> str:
    """Bearer token for the app's OWN service principal (for app-owned writes
    like the analytics log — NOT a persona). authenticate() returns a stable
    {'Authorization': 'Bearer <tok>'} header dict across SDK 0.57."""
    hdr = _app_client.config.authenticate() or {}
    auth_val = hdr.get("Authorization", "")
    return auth_val.split(" ", 1)[1] if " " in auth_val else auth_val


def get_persona(persona_key: str) -> Persona:
    """Load (and cache) a persona's SP credentials from the secret scope."""
    persona_key = persona_key.upper()
    if persona_key in _persona_cache:
        return _persona_cache[persona_key]
    if persona_key not in _PERSONA_SECRET_KEYS:
        raise ValueError(f"Unknown persona '{persona_key}'")
    cid_key, sec_key = _PERSONA_SECRET_KEYS[persona_key]
    persona = Persona(
        key=persona_key,
        label=PERSONA_LABELS[persona_key],
        client_id=_read_secret(cid_key),
        _secret=_read_secret(sec_key),
    )
    _persona_cache[persona_key] = persona
    return persona


def get_token(persona_key: str) -> str:
    """Return a valid OAuth M2M access token for the persona's SP (cached)."""
    persona_key = persona_key.upper()
    with _lock:
        cached = _token_cache.get(persona_key)
        if cached and cached[1] - 60 > time.time():
            return cached[0]

        persona = get_persona(persona_key)
        resp = requests.post(
            f"{DATABRICKS_HOST}/oidc/v1/token",
            auth=(persona.client_id, persona._secret),
            data={"grant_type": "client_credentials", "scope": "all-apis"},
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        token = payload["access_token"]
        expiry = time.time() + int(payload.get("expires_in", 3600))
        _token_cache[persona_key] = (token, expiry)
        return token
