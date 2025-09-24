from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Optional
import chardet

from .models import IndicatorRow
from .uploader import UploadMode


REQUIRED_COLUMNS = ["indicator", "type", "severity"]
OPTIONAL_COLUMNS = ["reputation", "expiration_date", "comment", "reliability"]
ALL_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS


def detect_file_encoding(file_path: Path) -> str:
    """
    Detect the encoding of a file using chardet.
    
    Returns the detected encoding or falls back to common encodings if detection fails.
    """
    try:
        with file_path.open("rb") as f:
            raw_data = f.read()
        
        if not raw_data:
            return "utf-8"
        
        # Use chardet to detect encoding
        detection_result = chardet.detect(raw_data)
        detected_encoding = detection_result.get("encoding")
        confidence = detection_result.get("confidence", 0)
        
        # If detection confidence is high enough, use detected encoding
        if detected_encoding and confidence > 0.7:
            return detected_encoding.lower()
        
        # Fall back to common encodings if detection is uncertain
        common_encodings = ["utf-8", "utf-8-sig", "iso-8859-1", "windows-1252", "cp1252"]
        
        for encoding in common_encodings:
            try:
                raw_data.decode(encoding)
                return encoding
            except UnicodeDecodeError:
                continue
        
        # Last resort: return utf-8 with error handling
        return "utf-8"
        
    except Exception:
        # If any error occurs, default to utf-8
        return "utf-8"


def load_csv_rows(file_path: Path, mode: UploadMode) -> List[IndicatorRow]:
    # Detect the file encoding
    detected_encoding = detect_file_encoding(file_path)
    
    try:
        with file_path.open("r", encoding=detected_encoding, newline="") as f:
            reader = csv.DictReader(f)
            header = [h.strip() for h in (reader.fieldnames or [])]
            missing = [c for c in REQUIRED_COLUMNS if c not in header]
            unexpected = [c for c in header if c not in ALL_COLUMNS]
            if missing:
                raise ValueError(f"Missing required columns: {', '.join(missing)}")
            if unexpected:
                raise ValueError(f"Unexpected columns: {', '.join(unexpected)}")

            rows: List[IndicatorRow] = []
            for row_num, raw in enumerate(reader, start=2):  # Start at 2 because row 1 is header
                data = {k: (raw.get(k) or "").strip() for k in ALL_COLUMNS}
                
                # Skip empty rows (where all required fields are empty)
                if not any(data.get(col) for col in REQUIRED_COLUMNS):
                    continue
                
                # Convert empty strings to None for optional fields only
                for key, value in list(data.items()):
                    if value == "" and key in OPTIONAL_COLUMNS:
                        data[key] = None
                
                # Validate that required fields are not empty
                missing_required = [col for col in REQUIRED_COLUMNS if not data.get(col)]
                if missing_required:
                    raise ValueError(f"Row {row_num}: Missing required values for columns: {', '.join(missing_required)}")
                
                rows.append(IndicatorRow(**{k: data.get(k) for k in ALL_COLUMNS}))

            return rows
    
    except UnicodeDecodeError as e:
        raise ValueError(
            f"Could not decode file '{file_path}' with detected encoding '{detected_encoding}'. "
            f"The file may be corrupted or have an unsupported encoding. "
            f"Original error: {str(e)}"
        ) from e
    except Exception as e:
        # Re-raise other exceptions as they are
        raise e

