from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    fqdn: str
    api_key_id: str
    api_key: str
    advanced: bool = True
    log_level: str = "INFO"

    @property
    def base_url(self) -> str:
        return f"https://{self.fqdn}"

    @property
    def csv_endpoint(self) -> str:
        return "/public_api/v1/indicators/insert_csv"

    @property
    def json_endpoint(self) -> str:
        return "/public_api/v1/indicators/insert_jsons"


def get_settings() -> Settings:
    """Load settings from environment or .env file."""
    load_dotenv(override=False)
    fqdn = os.environ.get("XDR_FQDN", "").strip()
    api_key_id = os.environ.get("XDR_API_KEY_ID", "").strip()
    api_key = os.environ.get("XDR_API_KEY", "").strip()
    advanced = os.environ.get("XDR_ADVANCED", "true").lower() in {"1", "true", "yes"}
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

    if not fqdn or not api_key_id or not api_key:
        pairs = [("XDR_FQDN", fqdn), ("XDR_API_KEY_ID", api_key_id), ("XDR_API_KEY", api_key)]
        missing = [name for name, value in pairs if not value]
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}."
        )

    return Settings(
        fqdn=fqdn,
        api_key_id=api_key_id,
        api_key=api_key,
        advanced=advanced,
        log_level=log_level,
    )

