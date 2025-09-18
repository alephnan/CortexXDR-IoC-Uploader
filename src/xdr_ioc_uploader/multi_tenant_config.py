from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class TenantConfig:
    """Configuration for a single XDR tenant."""
    name: str
    fqdn: str
    api_key_id: str
    api_key: str
    advanced: bool = True
    
    @property
    def base_url(self) -> str:
        return f"https://{self.fqdn}"
    
    @property
    def csv_endpoint(self) -> str:
        return "/public_api/v1/indicators/insert_csv"
    
    @property
    def json_endpoint(self) -> str:
        return "/public_api/v1/indicators/insert_jsons"


@dataclass
class MultiTenantSettings:
    """Settings for multi-tenant operations."""
    tenants: List[TenantConfig]
    log_level: str = "INFO"
    
    def get_tenant(self, name: str) -> Optional[TenantConfig]:
        """Get tenant configuration by name."""
        for tenant in self.tenants:
            if tenant.name == name:
                return tenant
        return None
    
    def get_tenants(self, names: Optional[List[str]] = None) -> List[TenantConfig]:
        """Get filtered list of tenant configurations."""
        if names is None:
            return self.tenants
        
        result = []
        for name in names:
            tenant = self.get_tenant(name)
            if tenant:
                result.append(tenant)
            else:
                raise ValueError(f"Tenant '{name}' not found in configuration")
        return result
    
    @property
    def tenant_names(self) -> List[str]:
        """Get list of all configured tenant names."""
        return [tenant.name for tenant in self.tenants]


def load_from_json(config_path: Path) -> MultiTenantSettings:
    """Load multi-tenant configuration from JSON file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with config_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    
    if "tenants" not in data:
        raise ValueError("Configuration file must contain 'tenants' key")
    
    tenants = []
    for tenant_data in data["tenants"]:
        # Validate required fields
        required_fields = ["name", "fqdn", "api_key_id", "api_key"]
        for field in required_fields:
            if field not in tenant_data:
                raise ValueError(f"Tenant configuration missing required field: {field}")
        
        tenants.append(TenantConfig(
            name=tenant_data["name"],
            fqdn=tenant_data["fqdn"],
            api_key_id=tenant_data["api_key_id"],
            api_key=tenant_data["api_key"],
            advanced=tenant_data.get("advanced", True)
        ))
    
    if not tenants:
        raise ValueError("No tenants configured")
    
    return MultiTenantSettings(
        tenants=tenants,
        log_level=data.get("log_level", "INFO").upper()
    )


def load_from_environment() -> MultiTenantSettings:
    """Load multi-tenant configuration from environment variables."""
    # Skip .env loading if SKIP_DOTENV is set (for testing)
    if not os.environ.get("SKIP_DOTENV"):
        load_dotenv(override=False)
    
    tenants = []
    tenant_index = 1
    
    # Look for numbered tenant configurations
    while True:
        prefix = f"TENANT{tenant_index}_"
        fqdn = os.environ.get(f"{prefix}XDR_FQDN", "").strip()
        
        if not fqdn:
            # Try without number for first tenant
            if tenant_index == 1:
                fqdn = os.environ.get("XDR_FQDN", "").strip()
                prefix = ""
            else:
                break
        
        api_key_id = os.environ.get(f"{prefix}XDR_API_KEY_ID", "").strip()
        api_key = os.environ.get(f"{prefix}XDR_API_KEY", "").strip()
        advanced = os.environ.get(f"{prefix}XDR_ADVANCED", "true").lower() in {"1", "true", "yes"}
        
        if not api_key_id or not api_key:
            if tenant_index == 1 and prefix == "":
                # Single tenant configuration
                break
            else:
                tenant_index += 1
                continue
        
        # Generate name from FQDN if not specified
        name = os.environ.get(f"{prefix}XDR_NAME", f"tenant{tenant_index}")
        
        tenants.append(TenantConfig(
            name=name,
            fqdn=fqdn,
            api_key_id=api_key_id,
            api_key=api_key,
            advanced=advanced
        ))
        
        tenant_index += 1
    
    if not tenants:
        raise RuntimeError(
            "No tenants configured. Use TENANT1_XDR_FQDN, TENANT1_XDR_API_KEY_ID, "
            "TENANT1_XDR_API_KEY for first tenant, TENANT2_* for second, etc."
        )
    
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    
    return MultiTenantSettings(tenants=tenants, log_level=log_level)


def get_multi_tenant_settings(config_file: Optional[Path] = None) -> MultiTenantSettings:
    """
    Load multi-tenant settings from configuration file or environment.
    
    Priority:
    1. Specified config file (if --config-file provided)
    2. Environment variables (.env file) - DEFAULT BEHAVIOR
    """
    # Only use JSON file if explicitly specified
    if config_file:
        return load_from_json(config_file)
    
    # Default: use environment variables (loads .env automatically via load_dotenv)
    return load_from_environment()