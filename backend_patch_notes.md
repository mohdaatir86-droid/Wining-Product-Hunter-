# Product Hunter v6.5 discovery correction

The v6.5 backend already contains multi-provider discovery functions. This commit adds a safe integration helper and parser diagnostics without overwriting the existing backend.

## Verification

1. Deploy the branch to Render.
2. Ensure at least one demand key exists: `ZENROWS_API_KEY`, `SCRAPEDO_API_KEY`, `BRIGHTDATA_API_KEY`, or `SCRAPINGANT_API_KEY`.
3. Run `POST /admin/hunt-now/home_decor`.
4. Read `GET /hunt-diagnostics/{run_id}`.
5. Confirm each provider shows `called`, `http_status`, `parser`, `records_extracted`, `records_normalized`, `latency_ms`, and `error`.

This commit does not claim live success; live success requires a real diagnostic response from Render.