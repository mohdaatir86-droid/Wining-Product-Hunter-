from __future__ import annotations
from typing import Any
import json
import re


def _first(value: Any, keys: tuple[str, ...], default: Any = None) -> Any:
    if not isinstance(value, dict):
        return default
    lower = {str(k).lower(): v for k, v in value.items()}
    for key in keys:
        found = lower.get(key.lower())
        if found not in (None, ""):
            return found
    return default


def _dict_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    if isinstance(value, dict):
        for key in ("products", "items", "results", "data", "product_results", "search_results", "organic_results"):
            rows = _dict_rows(value.get(key))
            if rows:
                return rows
        if _first(value, ("title", "name", "product_title", "productName", "asin")):
            return [value]
    return []


def _number(value: Any) -> float | None:
    if isinstance(value, dict):
        value = _first(value, ("amount", "value", "number", "price", "extracted_value"))
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", str(value).replace(",", ""))
    return float(match.group()) if match else None


def _amazon_html_rows(text: str) -> list[dict[str, Any]]:
    if not isinstance(text, str) or "data-component-type" not in text:
        return []
    rows: list[dict[str, Any]] = []
    pattern = re.compile(r'<div[^>]+data-component-type=["\']s-search-result["\'][^>]+data-asin=["\']([^"\']+)["\'][\s\S]*?</div>\s*</div>', re.I)
    for match in pattern.finditer(text):
        block = match.group(0)
        title = re.search(r'class=["\'][^"\']*(?:a-size-medium|a-size-base-plus)[^"\']*["\'][^>]*>([\s\S]*?)</span>', block, re.I)
        whole = re.search(r'a-price-whole[^>]*>([\d,]+)', block, re.I)
        fraction = re.search(r'a-price-fraction[^>]*>(\d+)', block, re.I)
        image = re.search(r'<img[^>]+(?:src|data-src)=["\']([^"\']+)', block, re.I)
        link = re.search(r'href=["\']([^"\']*/dp/[^"\']+)', block, re.I)
        row: dict[str, Any] = {"asin": match.group(1)}
        if title:
            row["title"] = re.sub(r"<[^>]+>", " ", title.group(1)).strip()
        if whole:
            row["price"] = f"{whole.group(1).replace(',', '')}.{fraction.group(1) if fraction else '00'}"
        if image:
            row["image"] = image.group(1)
        if link:
            row["url"] = link.group(1)
        if row.get("title") or row.get("asin"):
            rows.append(row)
    return rows


def extract_product_rows(payload: Any) -> tuple[list[dict[str, Any]], str]:
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", "ignore")
    if isinstance(payload, str):
        try:
            decoded = json.loads(payload)
            rows, parser = extract_product_rows(decoded)
            if rows:
                return rows, f"json:{parser}"
        except Exception:
            pass
        rows = _amazon_html_rows(payload)
        return rows, "amazon_html" if rows else "none"
    rows = _dict_rows(payload)
    return rows, "structured_json" if rows else "none"


def normalize_provider_row(row: dict[str, Any], provider: str) -> dict[str, Any]:
    title = str(_first(row, ("title", "name", "product_title", "productName", "productTitle", "itemTitle"), "") or "").strip()
    price = _number(_first(row, ("price", "current_price", "currentPrice", "sale_price", "salePrice", "selling_price", "amount")))
    rating = _number(_first(row, ("rating", "stars", "star_rating", "ratingValue")))
    reviews = _number(_first(row, ("reviews", "review_count", "reviewCount", "reviews_count", "ratingsTotal", "ratings_total")))
    image = _first(row, ("image", "image_url", "imageUrl", "thumbnail", "thumbnailUrl", "main_image"))
    if isinstance(image, dict):
        image = _first(image, ("url", "src", "link"))
    url = _first(row, ("url", "product_url", "productUrl", "detail_url", "detailPageURL", "link"))
    asin = _first(row, ("asin", "ASIN", "product_id", "productId", "sku"))
    rank = _first(row, ("best_seller_rank", "bestSellerRank", "sales_rank", "salesRank", "bsr"))
    if isinstance(rank, dict):
        rank = _first(rank, ("rank", "value", "position"))
    result = dict(row)
    if title: result["name"] = title
    if price is not None: result["price"] = price
    if rating is not None: result["rating"] = rating
    if reviews is not None: result["reviews"] = int(reviews)
    if image: result["image"] = image
    if url: result["url"] = url
    if asin: result["asin"] = asin
    if rank is not None: result["best_seller_rank"] = rank
    result["source_provider"] = provider
    return result


def normalize_provider_payload(payload: Any, provider: str) -> tuple[list[dict[str, Any]], str]:
    rows, parser = extract_product_rows(payload)
    normalized = [normalize_provider_row(row, provider) for row in rows]
    return normalized, parser
