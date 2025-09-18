from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def emit_run_artifact(action: str, payload: Dict[str, Any], tenant_name: Optional[str] = None) -> Path:
    """Emit run artifact with optional tenant name for multi-tenant operations."""
    if tenant_name:
        path = REPORTS_DIR / f"{_ts()}-{action}-{tenant_name}.json"
    else:
        path = REPORTS_DIR / f"{_ts()}-{action}.json"
    
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def emit_multi_tenant_artifact(action: str, payload: Dict[str, Any]) -> List[Path]:
    """
    Emit both consolidated and per-tenant artifacts for multi-tenant operations.
    
    Returns list of paths: [consolidated_report, tenant1_report, tenant2_report, ...]
    """
    paths = []
    
    # Emit consolidated report
    consolidated_path = emit_run_artifact(action, payload)
    paths.append(consolidated_path)
    
    # Emit per-tenant reports
    if "tenant_results" in payload:
        for tenant_result in payload["tenant_results"]:
            tenant_name = tenant_result.get("tenant_name")
            if tenant_name:
                tenant_payload = {
                    "tenant_name": tenant_name,
                    "timestamp": payload.get("timestamp"),
                    "action": action,
                    "result": tenant_result
                }
                tenant_path = emit_run_artifact(action, tenant_payload, tenant_name)
                paths.append(tenant_path)
    
    return paths


def write_errors_csv(errors: Iterable[Dict[str, Any]], tenant_name: Optional[str] = None) -> Path:
    """Write errors to CSV with optional tenant name prefix."""
    if tenant_name:
        path = REPORTS_DIR / f"errors-{tenant_name}.csv"
    else:
        path = REPORTS_DIR / "errors.csv"
    
    # Normalize common fields
    rows: List[Dict[str, Any]] = []
    for e in errors:
        if isinstance(e, dict):
            rows.append(e)
        else:
            rows.append({"error": str(e)})
    
    # Collect all keys
    fieldnames: List[str] = sorted({k for row in rows for k in row.keys()}) or ["error"]
    
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    
    return path


def write_multi_tenant_errors_csv(tenant_results: List[Dict[str, Any]]) -> List[Path]:
    """Write error CSV files for each tenant that has errors."""
    paths = []
    
    for tenant_result in tenant_results:
        tenant_name = tenant_result.get("tenant_name")
        all_errors = []
        
        # Collect all errors and validation errors
        if tenant_result.get("errors"):
            all_errors.extend(tenant_result["errors"])
        if tenant_result.get("validation_errors"):
            all_errors.extend(tenant_result["validation_errors"])
        
        # Write errors if any exist
        if all_errors and tenant_name:
            path = write_errors_csv(all_errors, tenant_name)
            paths.append(path)
    
    return paths

