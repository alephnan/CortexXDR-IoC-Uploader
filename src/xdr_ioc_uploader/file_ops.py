from __future__ import annotations

import csv
import ipaddress
import re
import shutil
import string
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .csv_io import (
    ALL_COLUMNS,
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
    detect_file_encoding,
    load_csv_rows,
)
from .models import IndicatorRow, IndicatorType, Reliability, Reputation, Severity
from .transformers import build_csv_request_data
from .uploader import UploadMode


HASH_LENGTHS = {32, 40, 64}
HEX_CHARS = set(string.hexdigits)
PATH_SEPARATORS = {"/", "\\"}
WINDOWS_DRIVE_RE = re.compile(r"^[a-zA-Z]:[\\/]")
DOMAIN_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z", re.IGNORECASE)


class FileOperationError(Exception):
    """Raised for invalid parameters or processing issues in file operations."""


@dataclass
class FileOperationResult:
    rows: List[IndicatorRow]
    summary: Dict[str, object]


def load_rows(path: Path) -> Tuple[List[IndicatorRow], str]:
    """Load CSV rows for preprocessing commands and return detected encoding."""

    rows = load_csv_rows(path, mode=UploadMode.csv)
    encoding = detect_file_encoding(path) or "utf-8"
    return rows, encoding


def write_rows(rows: List[IndicatorRow], output: Path, encoding: str) -> None:
    """Write rows back to CSV using the canonical column order."""

    data = build_csv_request_data(rows)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding=encoding, newline="") as handle:
        handle.write(data)


def create_backup(path: Path) -> Path:
    """Create a `.bak` backup, avoiding collisions by appending a counter."""

    idx = 0
    candidate = path.with_suffix(path.suffix + ".bak")
    while candidate.exists():
        idx += 1
        candidate = path.with_suffix(path.suffix + f".bak{idx}")
    shutil.copy2(path, candidate)
    return candidate


def load_rows_for_classification(path: Path) -> Tuple[List[IndicatorRow], str, List[str]]:
    """Load rows but tolerate empty/invalid type values for classification."""

    encoding = detect_file_encoding(path) or "utf-8"
    rows: List[IndicatorRow] = []
    original_types: List[str] = []

    with path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        header = [h.strip() for h in (reader.fieldnames or [])]
        missing = [c for c in REQUIRED_COLUMNS if c not in header]
        unexpected = [c for c in header if c not in ALL_COLUMNS]
        if missing:
            raise FileOperationError(f"Missing required columns: {', '.join(missing)}")
        if unexpected:
            raise FileOperationError(f"Unexpected columns: {', '.join(unexpected)}")

        valid_types = {t.value for t in IndicatorType}

        for row_num, raw in enumerate(reader, start=2):
            data = {col: (raw.get(col) or "").strip() for col in ALL_COLUMNS}

            if not any(data.get(col) for col in REQUIRED_COLUMNS):
                continue

            missing_required = [
                col for col in REQUIRED_COLUMNS if col != "type" and not data.get(col)
            ]
            if missing_required:
                raise FileOperationError(
                    f"Row {row_num}: Missing required values for columns: {', '.join(missing_required)}"
                )

            original_type = data.get("type") or ""
            normalized_type = original_type.strip().upper()
            placeholder_type = normalized_type if normalized_type in valid_types else IndicatorType.FILENAME.value

            for key in OPTIONAL_COLUMNS:
                if data.get(key) == "":
                    data[key] = None

            row_model = IndicatorRow(
                indicator=data["indicator"],
                type=placeholder_type,
                severity=data["severity"],
                reputation=data.get("reputation"),
                expiration_date=data.get("expiration_date"),
                comment=data.get("comment"),
                reliability=data.get("reliability"),
            )

            rows.append(row_model)
            original_types.append(original_type)

    return rows, encoding, original_types


def classify_rows(
    rows: List[IndicatorRow],
    *,
    only_empty: bool = False,
    force: bool = False,
    original_types: Optional[List[str]] = None,
) -> FileOperationResult:
    """Infer indicator types and optionally overwrite existing values."""

    updated_rows: List[IndicatorRow] = []
    type_counts: Counter[str] = Counter()

    updated = 0
    unchanged = 0
    skipped_only_empty = 0
    conflicts = 0
    conflicts_updated = 0
    conflicts_skipped = 0
    forced_updates = 0
    ambiguous = 0
    filled_from_empty = 0

    for index, row in enumerate(rows):
        detected_type, confident = _classify_indicator(row.indicator)
        type_counts[detected_type] += 1
        if not confident:
            ambiguous += 1

        current_source = (
            (original_types[index] if original_types is not None else row.type) or ""
        )
        current_type = current_source.strip().upper()
        has_existing = current_type != ""

        if only_empty and has_existing:
            skipped_only_empty += 1
            if current_type == detected_type:
                unchanged += 1
            updated_rows.append(row)
            continue

        if has_existing:
            if current_type == detected_type:
                unchanged += 1
                updated_rows.append(row)
                continue

            conflicts += 1
            if not confident and not force:
                conflicts_skipped += 1
                updated_rows.append(row)
                continue

            if not confident and force:
                forced_updates += 1

            new_row = row.model_copy(update={"type": detected_type})
            updated_rows.append(new_row)
            updated += 1
            conflicts_updated += 1
            continue

        # No existing type
        new_row = row.model_copy(update={"type": detected_type})
        updated_rows.append(new_row)
        updated += 1
        filled_from_empty += 1

    summary = {
        "total_rows": len(rows),
        "updated": updated,
        "unchanged": unchanged,
        "skipped_only_empty": skipped_only_empty,
        "conflicts": conflicts,
        "conflicts_updated": conflicts_updated,
        "conflicts_skipped": conflicts_skipped,
        "forced_updates": forced_updates,
        "ambiguous_assignments": ambiguous,
        "filled_from_empty": filled_from_empty,
        "detected_type_counts": dict(sorted(type_counts.items())),
    }

    return FileOperationResult(rows=updated_rows, summary=summary)


def apply_reputation(
    rows: List[IndicatorRow],
    default_value: str,
    overrides: Dict[str, Optional[str]],
    *,
    only_empty: bool = False,
    apply_default_globally: bool = True,
) -> FileOperationResult:
    return _apply_field(
        rows,
        field="reputation",
        default_value=default_value,
        overrides=overrides,
        only_empty=only_empty,
        allow_none=True,
        normalizer=_normalize_reputation,
        apply_default_globally=apply_default_globally,
    )


def apply_severity(
    rows: List[IndicatorRow],
    default_value: str,
    overrides: Dict[str, Optional[str]],
    *,
    only_empty: bool = False,
    apply_default_globally: bool = True,
) -> FileOperationResult:
    return _apply_field(
        rows,
        field="severity",
        default_value=default_value,
        overrides=overrides,
        only_empty=only_empty,
        allow_none=False,
        normalizer=_normalize_severity,
        apply_default_globally=apply_default_globally,
    )


def apply_comment(
    rows: List[IndicatorRow],
    default_value: str,
    overrides: Dict[str, Optional[str]],
    *,
    only_empty: bool = False,
    apply_default_globally: bool = True,
) -> FileOperationResult:
    return _apply_field(
        rows,
        field="comment",
        default_value=default_value,
        overrides=overrides,
        only_empty=only_empty,
        allow_none=True,
        normalizer=_normalize_comment,
        apply_default_globally=apply_default_globally,
    )


def apply_reliability(
    rows: List[IndicatorRow],
    default_value: str,
    overrides: Dict[str, Optional[str]],
    *,
    only_empty: bool = False,
    apply_default_globally: bool = True,
) -> FileOperationResult:
    return _apply_field(
        rows,
        field="reliability",
        default_value=default_value,
        overrides=overrides,
        only_empty=only_empty,
        allow_none=True,
        normalizer=_normalize_reliability,
        apply_default_globally=apply_default_globally,
    )


def _apply_field(
    rows: List[IndicatorRow],
    *,
    field: str,
    default_value: Optional[str],
    overrides: Dict[str, Optional[str]],
    only_empty: bool,
    allow_none: bool,
    normalizer,
    apply_default_globally: bool = True,
) -> FileOperationResult:
    try:
        normalized_default = normalizer(default_value)
    except ValueError as exc:
        raise FileOperationError(str(exc)) from exc

    normalized_overrides: Dict[str, Optional[str]] = {}
    for key, value in overrides.items():
        if value is None:
            continue
        try:
            normalized_overrides[key] = normalizer(value)
        except ValueError as exc:
            raise FileOperationError(f"{key}: {exc}") from exc

    if normalized_default is None and not normalized_overrides and not allow_none:
        raise FileOperationError("A non-empty value is required for this command.")

    updated_rows: List[IndicatorRow] = []
    updated = 0
    unchanged = 0
    skipped_only_empty = 0
    default_targets = 0
    cleared = 0
    override_counts: Dict[str, int] = {key: 0 for key in normalized_overrides}

    for row in rows:
        current_value = getattr(row, field)
        if only_empty and not _is_empty(current_value):
            skipped_only_empty += 1
            updated_rows.append(row)
            continue

        target_value: Optional[str]
        if row.type in normalized_overrides:
            target_value = normalized_overrides[row.type]
            override_counts[row.type] += 1
        else:
            if not apply_default_globally:
                unchanged += 1
                updated_rows.append(row)
                continue
            target_value = normalized_default
            default_targets += 1

        if target_value is None and not allow_none:
            raise FileOperationError("Value cannot be empty for this field.")

        if _values_equal(current_value, target_value):
            unchanged += 1
            updated_rows.append(row)
            continue

        if target_value is None:
            cleared += 1

        new_row = row.model_copy(update={field: target_value})
        updated_rows.append(new_row)
        updated += 1

    summary = {
        "total_rows": len(rows),
        "updated": updated,
        "unchanged": unchanged,
        "skipped_only_empty": skipped_only_empty,
        "default_assignments": default_targets,
        "overrides_applied": {k: v for k, v in override_counts.items() if v > 0},
        "cleared": cleared,
    }

    return FileOperationResult(rows=updated_rows, summary=summary)


def _classify_indicator(indicator: str) -> Tuple[str, bool]:
    value = indicator.strip()
    if not value:
        return IndicatorType.FILENAME.value, False

    if _looks_like_hash(value):
        return IndicatorType.HASH.value, True

    if _looks_like_ip(value):
        return IndicatorType.IP.value, True

    if _looks_like_path(value):
        return IndicatorType.PATH.value, True

    if _looks_like_domain(value):
        return IndicatorType.DOMAIN_NAME.value, True

    if _looks_like_filename(value):
        return IndicatorType.FILENAME.value, True

    return IndicatorType.FILENAME.value, False


def _looks_like_hash(value: str) -> bool:
    compact = value.replace(" ", "")
    return len(compact) in HASH_LENGTHS and all(ch in HEX_CHARS for ch in compact)


def _looks_like_ip(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def _looks_like_path(value: str) -> bool:
    if WINDOWS_DRIVE_RE.match(value):
        return True
    if value.startswith("\\\\") or value.startswith("//"):
        return True
    if value.startswith("~/") or value.startswith("~\\"):
        return True
    return any(sep in value for sep in PATH_SEPARATORS)


def _looks_like_domain(value: str) -> bool:
    if any(sep in value for sep in PATH_SEPARATORS) or " " in value:
        return False

    candidate = value.rstrip(".")
    if len(candidate) > 253 or candidate.count(".") == 0:
        return False

    labels = candidate.split(".")
    if labels[-1].isdigit():
        return False

    for label in labels:
        if not DOMAIN_LABEL_RE.match(label):
            return False

    return True


def _looks_like_filename(value: str) -> bool:
    if any(sep in value for sep in PATH_SEPARATORS):
        return False
    if value in (".", ".."):
        return False
    return "." in value


def _normalize_reputation(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    text = value.strip()
    if not text:
        return None

    lowered = text.lower()
    if lowered in {"no reputation", "none", "unset", "clear"}:
        return None

    upper = text.upper()
    valid = {rep.value for rep in Reputation}
    if upper not in valid:
        raise ValueError(
            "Reputation must be one of: " + ", ".join(sorted(valid)) + ", or 'no reputation'"
        )
    return upper


def _normalize_severity(value: Optional[str]) -> str:
    if value is None:
        raise ValueError("Severity value is required.")

    text = value.strip()
    if not text:
        raise ValueError("Severity value cannot be empty.")

    upper = text.upper()
    if upper == "INFORMATIONAL":
        upper = "INFO"
    if upper == "MEIDUM":
        upper = "MEDIUM"

    valid = {sev.value for sev in Severity}
    if upper not in valid:
        raise ValueError("Severity must be one of: " + ", ".join(sorted(valid | {"INFORMATIONAL"})))
    return upper


def _normalize_comment(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value


def _normalize_reliability(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    text = value.strip()
    if not text:
        return None

    upper = text.upper()
    valid = {rel.value for rel in Reliability}
    if upper not in valid:
        raise ValueError("Reliability must be one of: " + ", ".join(sorted(valid)))
    return upper


def _is_empty(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _values_equal(current: object, target: object) -> bool:
    if isinstance(current, str) or isinstance(target, str):
        current_norm = current if isinstance(current, str) else ("" if current is None else str(current))
        target_norm = target if isinstance(target, str) else ("" if target is None else str(target))
        return current_norm == target_norm
    return current == target


def resolve_default_output(input_path: Path, command_name: str) -> Path:
    """Generate a default output path when --output is not provided."""

    suffix = input_path.suffix or ".csv"
    slug = command_name.replace("file-", "")
    return input_path.with_name(f"{input_path.stem}-{slug}{suffix}")
