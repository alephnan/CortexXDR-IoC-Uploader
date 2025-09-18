from __future__ import annotations

import hashlib
import json
import secrets
import string
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests

from .config import Settings


class XdrApiClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()

    def _standard_headers(self) -> Dict[str, str]:
        return {
            "Authorization": self.settings.api_key,
            "x-xdr-auth-id": self.settings.api_key_id,
            "Content-Type": "application/json",
        }

    def _advanced_headers(self) -> Dict[str, str]:
        # Generate a 64-character nonce using secure random generation
        nonce = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(64))
        
        # Get the current timestamp in milliseconds since epoch (UTC)
        timestamp = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        
        # Create the authentication string by concatenating API key, nonce, and timestamp
        auth_string = f"{self.settings.api_key}{nonce}{timestamp}"
        
        # Compute SHA256 hash of the authentication string
        signature = hashlib.sha256(auth_string.encode("utf-8")).hexdigest()
        
        return {
            "Authorization": signature,
            "x-xdr-auth-id": self.settings.api_key_id,
            "x-xdr-timestamp": timestamp,
            "x-xdr-nonce": nonce,
            "Content-Type": "application/json",
        }

    def _headers(self) -> Dict[str, str]:
        return self._advanced_headers() if self.settings.advanced else self._standard_headers()

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.settings.base_url}{path}"
        resp = self.session.post(url, headers=self._headers(), data=json.dumps(payload), timeout=60)
        resp.raise_for_status()
        return resp.json()

    def insert_csv(self, request_data: str, validate: bool) -> Dict[str, Any]:
        payload = {"request_data": request_data, "validate": validate}
        return self._post(self.settings.csv_endpoint, payload)

    def insert_jsons(self, objects: List[Dict[str, Any]], validate: bool) -> Dict[str, Any]:
        payload = {"request_data": objects, "validate": validate}
        return self._post(self.settings.json_endpoint, payload)

    def test_authentication(self) -> Dict[str, Any]:
        """Test authentication by sending a minimal validation request."""
        # Use a minimal test payload that should always validate
        test_payload = {
            "request_data": "indicator,type,severity,reputation,expiration_date,comment\n127.0.0.1,IP,LOW,UNKNOWN,,auth test",
            "validate": True
        }
        return self._post(self.settings.csv_endpoint, test_payload)

