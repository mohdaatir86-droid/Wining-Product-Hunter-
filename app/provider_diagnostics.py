from __future__ import annotations
from dataclasses import asdict, dataclass, field
from typing import Any
import time

@dataclass
class ProviderDiagnostic:
    provider: str
    kind: str = "discovery"
    called: bool = False
    request_attempted: bool = False
    http_status: int | None = None
    response_type: str | None = None
    response_bytes: int | None = None
    records_extracted: int = 0
    records_normalized: int = 0
    parser: str | None = None
    status: str = "not_run"
    error: str | None = None
    latency_ms: float | None = None
    last_successful_run: str | None = None
    fields: list[str] = field(default_factory=list)

    def start(self) -> float:
        self.called = True
        self.request_attempted = True
        return time.perf_counter()

    def finish(self, started: float, *, status: str, records: int = 0,
               parser: str | None = None, error: str | None = None,
               response: Any = None, http_status: int | None = None,
               fields: list[str] | None = None) -> None:
        self.status = status
        self.records_extracted = records
        self.parser = parser
        self.error = error
        self.http_status = http_status
        self.latency_ms = round((time.perf_counter() - started) * 1000, 1)
        if response is not None:
            self.response_type = type(response).__name__
            if isinstance(response, (str, bytes, bytearray)):
                self.response_bytes = len(response)
        self.fields = sorted(set(fields or []))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
