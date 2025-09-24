from __future__ import annotations

import csv
import io
from typing import List, Dict, Any

from .models import IndicatorRow, IndicatorTypeJson


CSV_COLUMNS = ["indicator", "type", "severity", "reputation", "expiration_date", "comment", "reliability"]


def build_csv_request_data(rows: List[IndicatorRow]) -> str:
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(CSV_COLUMNS)
    for row in rows:
        # expiration_date: allow "Never" string or epoch ms integer
        exp = row.expiration_date
        writer.writerow([
            row.indicator,
            row.type,
            row.severity,
            row.reputation or "",
            exp if exp is not None else "",
            row.comment or "",
            row.reliability or "",
        ])
    return output.getvalue()


def build_json_objects(rows: List[IndicatorRow]) -> List[Dict[str, Any]]:
    objects: List[Dict[str, Any]] = []
    for row in rows:
        if row.type == "PATH":
            raise ValueError("Type PATH is not supported in JSON mode; use CSV mode.")
        if row.type not in {t.value for t in IndicatorTypeJson}:
            raise ValueError(f"Unsupported JSON type: {row.type}")
        exp = row.expiration_date
        if isinstance(exp, str) and exp == "Never":
            exp_json = None
        else:
            exp_json = exp
        obj = {
            "indicator": row.indicator,
            "type": row.type,
            "severity": row.severity,
            "reputation": row.reputation,
            "expiration_date": exp_json,
            "comment": row.comment,
            "reliability": row.reliability,
        }
        # Remove None values
        objects.append({k: v for k, v in obj.items() if v is not None})
    return objects

