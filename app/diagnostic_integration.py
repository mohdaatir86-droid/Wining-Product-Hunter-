from __future__ import annotations

from typing import Any, Callable
from datetime import datetime, timezone
import inspect

from .provider_diagnostics import ProviderDiagnostic
from .provider_parsers import normalize_provider_payload


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def execute_provider(name: str, request: Callable[[], Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    diagnostic = ProviderDiagnostic(provider=name)
    started = diagnostic.start()
    try:
        payload = request()
        if inspect.isawaitable(payload):
            payload = await payload
        rows, parser = normalize_provider_payload(payload, name)
        fields = sorted({key for row in rows[:20] for key in row.keys()})
        status = "success" if rows else "parser_empty"
        diagnostic.finish(
            started,
            status=status,
            records=len(rows),
            parser=parser,
            response=payload,
            fields=fields,
        )
        diagnostic.records_normalized = len(rows)
        diagnostic.last_successful_run = utc_now() if rows else None
        return rows, diagnostic.to_dict()
    except Exception as exc:
        diagnostic.finish(
            started,
            status="error",
            error=str(exc)[:1000],
        )
        return [], diagnostic.to_dict()


def no_provider_diagnostic(names: list[str]) -> list[dict[str, Any]]:
    return [ProviderDiagnostic(provider=name, status="not_configured").to_dict() for name in names]
