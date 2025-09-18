from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import requests

from .api_client import XdrApiClient
from .config import Settings
from .models import IndicatorRow
from .rate_limiter import TokenBucket
from .transformers import build_csv_request_data, build_json_objects


class UploadMode(str, Enum):
    csv = "csv"
    json = "json"


@dataclass
class UploadResult:
    succeeded: int
    failed: int
    errors: List[Dict[str, Any]]


class Uploader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = XdrApiClient(settings)
        self.bucket = TokenBucket(rate_per_second=10, capacity=10)

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        retry=retry_if_exception(lambda e: _should_retry(e)),
        reraise=True,
    )
    def _insert_csv(self, request_data: str, validate: bool) -> Dict[str, Any]:
        return self.client.insert_csv(request_data=request_data, validate=validate)

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        retry=retry_if_exception(lambda e: _should_retry(e)),
        reraise=True,
    )
    def _insert_jsons(self, objects: List[Dict[str, Any]], validate: bool) -> Dict[str, Any]:
        return self.client.insert_jsons(objects=objects, validate=validate)

    def validate_csv(self, request_data: str) -> Dict[str, Any]:
        self.bucket.consume()
        return self._insert_csv(request_data=request_data, validate=True)

    def validate_json(self, objects: List[Dict[str, Any]]) -> Dict[str, Any]:
        self.bucket.consume()
        return self._insert_jsons(objects=objects, validate=True)

    def commit_csv(self, rows: List[IndicatorRow], batch_size: int) -> Dict[str, Any]:
        total_succeeded = 0
        total_failed = 0
        all_errors: List[Dict[str, Any]] = []
        for batch in _chunks(rows, batch_size):
            self.bucket.consume()
            request_data = build_csv_request_data(batch)
            reply = self._insert_csv(request_data=request_data, validate=False)
            
            # XDR API returns {"reply": True} for successful uploads
            # If reply is True and no errors, count the batch size as succeeded
            if reply.get("reply") is True:
                succeeded = len(batch)
                failed = 0
            else:
                # Handle error cases - might have error details in the response
                succeeded = 0
                failed = len(batch)
                if "errors" in reply or "validation_errors" in reply:
                    errors = reply.get("errors") or reply.get("validation_errors") or []
                    if isinstance(errors, list):
                        all_errors.extend(errors)
            
            total_succeeded += succeeded
            total_failed += failed
            
        return {"succeeded": total_succeeded, "failed": total_failed, "errors": all_errors}

    def commit_json(self, rows: List[IndicatorRow], batch_size: int) -> Dict[str, Any]:
        total_succeeded = 0
        total_failed = 0
        all_errors: List[Dict[str, Any]] = []
        json_objects = build_json_objects(rows)
        for batch in _chunks(json_objects, batch_size):
            self.bucket.consume()
            reply = self._insert_jsons(objects=batch, validate=False)
            
            # XDR API returns {"reply": True} for successful uploads
            # If reply is True and no errors, count the batch size as succeeded
            if reply.get("reply") is True:
                succeeded = len(batch)
                failed = 0
            else:
                # Handle error cases - might have error details in the response
                succeeded = 0
                failed = len(batch)
                if "errors" in reply or "validation_errors" in reply:
                    errors = reply.get("errors") or reply.get("validation_errors") or []
                    if isinstance(errors, list):
                        all_errors.extend(errors)
            
            total_succeeded += succeeded
            total_failed += failed
            
        return {"succeeded": total_succeeded, "failed": total_failed, "errors": all_errors}


def _chunks(seq: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        return status == 429 or 500 <= status < 600
    return False

