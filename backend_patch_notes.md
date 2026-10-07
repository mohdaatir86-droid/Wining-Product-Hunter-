# Product Hunter v6.5 discovery correction

This patch preserves the current v6.5 architecture and adds a provider-safe parsing/diagnostic layer.

## Deploy

1. Deploy this branch to Render.
2. Keep `DATAFORSEO` optional; HTTP 401 must not block discovery.
3. Ensure at least one discovery key exists in Render: `ZENROWS_API_KEY`, `SCRAPEDO_API_KEY`, `BRIGHTDATA_API_KEY`, or `SCRAPINGANT_API_KEY`.
4. Run `POST /admin/hunt-now/home_decor` for a synchronous diagnostic run.
5. Read `/hunt-diagnostics/{run_id}`, `/source-health`, and `/pipeline/latest`.

## Interpret

- `401`/`403`: provider credential/account problem.
- `200` plus `parser_empty`: provider responded but its payload shape was not recognized.
- `records_extracted > 0` and `records_normalized > 0`: discovery is proven; only then inspect category/scoring rejection.
- `no_provider_configured`: Render environment variables are missing.

No deployment is claimed until a live diagnostic returns provider records.
