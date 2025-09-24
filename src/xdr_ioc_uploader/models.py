from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, field_validator


class IndicatorType(str, Enum):
    HASH = "HASH"
    IP = "IP"
    PATH = "PATH"  # CSV only
    DOMAIN_NAME = "DOMAIN_NAME"
    FILENAME = "FILENAME"


class IndicatorTypeJson(str, Enum):
    HASH = "HASH"
    IP = "IP"
    DOMAIN_NAME = "DOMAIN_NAME"
    FILENAME = "FILENAME"


class Severity(str, Enum):
    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Reputation(str, Enum):
    GOOD = "GOOD"
    BAD = "BAD"
    SUSPICIOUS = "SUSPICIOUS"
    UNKNOWN = "UNKNOWN"


class Reliability(str, Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    G = "G"


class IndicatorRow(BaseModel):
    indicator: str
    type: str
    severity: str
    reputation: Optional[str] = None
    expiration_date: Optional[Union[str, int]] = None  # epoch ms, ISO, or "Never" for CSV
    comment: Optional[str] = None
    reliability: Optional[str] = None

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        v_up = v.strip().upper()
        # We accept PATH here; transformers will restrict for JSON
        if v_up not in {t.value for t in IndicatorType}:
            raise ValueError(f"Invalid type: {v}")
        return v_up

    @field_validator("severity")
    @classmethod
    def validate_severity(cls, v: str) -> str:
        v_up = v.strip().upper()
        if v_up not in {s.value for s in Severity}:
            raise ValueError(f"Invalid severity: {v}")
        return v_up

    @field_validator("reputation")
    @classmethod
    def validate_reputation(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v_up = v.strip().upper()
        if v_up not in {r.value for r in Reputation}:
            raise ValueError(f"Invalid reputation: {v}")
        return v_up

    @field_validator("reliability")
    @classmethod
    def validate_reliability(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v_up = v.strip().upper()
        if v_up not in {r.value for r in Reliability}:
            raise ValueError(f"Invalid reliability: {v}")
        return v_up

    @field_validator("expiration_date")
    @classmethod
    def normalize_expiration(cls, v: Optional[Union[str, int]]) -> Optional[Union[str, int]]:
        if v is None:
            return v
        if isinstance(v, int):
            # Assume already epoch ms
            return v
        v_str = str(v).strip()
        if v_str.lower() == "never":
            return "Never"
        # Try ISO-8601 to epoch ms
        try:
            dt = datetime.fromisoformat(v_str.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except ValueError:
            # If pure digits, assume epoch seconds or ms
            if v_str.isdigit():
                num = int(v_str)
                return num if num > 10_000_000_000 else num * 1000
        raise ValueError("Invalid expiration_date; use epoch ms, ISO-8601, or 'Never'")

