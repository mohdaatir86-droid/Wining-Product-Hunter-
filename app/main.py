"""
Product Hunter API v5.4.0
Primary source: Crawlbase (Amazon SERP demand + AliExpress SERP supplier)
Fallbacks (only used when Crawlbase returns nothing): RapidAPI, Apify
Categories: kitchen_utility, home_decor
"""
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from difflib import SequenceMatcher
from urllib.parse import quote_plus
import os, re, unicodedata, httpx, sqlite3, hashlib, json, asyncio, time
from datetime import datetime, timezone

VERSION = "5.4.0"
DB_PATH = os.getenv("PRODUCT_HUNTER_DB", "product_hunter.db")

app = FastAPI(title="Product Hunter API", version=VERSION)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False,
                   allow_methods=["*"], allow_headers=["*"])

# ---------------------------------------------------------------- config
def env_int(name, default):
    try: return int(os.getenv(name, default))
    except Exception: return default

def env_float(name, default):
    try: return float(os.getenv(name, default))
    except Exception: return default

CRAWLBASE_TOKEN = os.getenv("CRAWLBASE_TOKEN")
CRAWLBASE_ENDPOINT = "https://api.crawlbase.com/"
QUERIES_PER_HUNT = env_int("QUERIES_PER_HUNT", 5)
AMAZON_PAGES = env_int("CRAWLBASE_AMAZON_PAGES", 1)
TARGETED_SUPPLIER_SEARCHES = env_int("TARGETED_SUPPLIER_SEARCHES", 8)
MATCH_MIN = env_float("MATCH_MIN", 0.35)
SHIPPING_ESTIMATE = env_float("SHIPPING_ESTIMATE", 0.0)   # added to supplier price to form COGS
AUTO_HUNT_HOURS = env_float("AUTO_HUNT_HOURS", 0)          # 0 = scheduler off

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_AMAZON_HOST = os.getenv("RAPIDAPI_AMAZON_HOST", "")
RAPIDAPI_AMAZON_URL = os.getenv("RAPIDAPI_AMAZON_URL", "")
RAPIDAPI_ALIEXPRESS_HOST = os.getenv("RAPIDAPI_ALIEXPRESS_HOST", "aliexpress-api2.p.rapidapi.com")
RAPIDAPI_ALIEXPRESS_URL = os.getenv("RAPIDAPI_ALIEXPRESS_URL", "https://aliexpress-api2.p.rapidapi.com/search")
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")
APIFY_AMAZON = os.getenv("APIFY_DEMAND_ACTOR_ID", "prodiger~amazon-product-scraper")
APIFY_ALIEXPRESS = os.getenv("APIFY_SUPPLIER_ACTOR_ID", "crawlerbros~aliexpress-scraper")

FILTERS = {"min_price": 40.0, "max_price": 150.0, "preferred_min": 50.0, "preferred_max": 80.0,
           "max_cogs": 35.0, "min_markup": 3.0, "min_rating": 4.0, "min_reviews": 50, "min_score": 60.0}

CATEGORY_LABEL = {"kitchen_utility": "Kitchen Utility", "home_decor": "Home Décor"}

# Specific product-type queries, rotated between hunts so each run explores new ground.
CATEGORY_QUERIES = {
 "kitchen_utility": ["kitchen organizer", "pantry organizer", "drawer organizer kitchen", "sink caddy organizer",
                     "spice rack organizer", "under sink organizer", "pot lid organizer", "dish drying rack",
                     "kitchen utensil holder", "countertop organizer", "fridge organizer bins", "knife block organizer",
                     "cutting board organizer", "pan organizer rack", "kitchen storage containers", "meal prep tools",
                     "vegetable chopper manual", "kitchen gadget", "bakeware organizer", "paper towel holder"],
 "home_decor": ["wall decor", "table decor", "living room decor", "bedroom decor", "decorative storage basket",
                "boho wall hanging", "decorative tray", "throw pillow covers", "artificial plants decor",
                "macrame wall hanging", "wall shelves decor", "picture frame set", "decorative bookends",
                "planter pot indoor", "woven storage basket", "minimalist home decor", "aesthetic room decor",
                "entryway decor", "bathroom decor set", "sculpture decor"],
}
CATEGORY_SIGNALS = {
 "kitchen_utility": ["kitchen", "pantry", "drawer", "sink", "spice", "utensil", "cutting", "dish", "pot", "pan", "lid",
                     "cookware", "countertop", "fridge", "refrigerator", "cooking", "baking", "bakeware", "knife",
                     "peeler", "colander", "strainer", "container", "organizer", "rack", "holder", "caddy",
                     "measuring", "spatula", "chopper", "grater", "storage", "towel", "meal prep", "slicer"],
 "home_decor": ["decor", "decorative", "wall", "art", "vase", "pillow", "cushion", "throw", "blanket", "rug", "curtain",
                "frame", "planter", "plant", "basket", "tray", "candle", "sculpture", "statue", "shelf", "shelves",
                "macrame", "tapestry", "hanging", "centerpiece", "bookend", "artificial", "faux", "boho", "aesthetic",
                "room", "bedroom", "living", "mantel", "entryway", "bathroom", "storage", "ornament", "figurine"],
}
OUT_OF_SCOPE = ["shirt", "dress", "shoe", "shoes", "sneaker", "toy", "toys", "dog", "puppy", "cat litter", "pet",
                "makeup", "cosmetic", "phone case", "iphone", "car ", "automotive", "jewelry", "necklace", "earring",
                "watch band", "baby bottle", "costume", "book"]

HARD_REJECT = {
 "electronics": [r"\belectronic", r"\belectric\b", r"\bbatter(y|ies)\b", r"\brechargeable\b", r"\bbluetooth\b",
                 r"\busb\b", r"\bsmart\b", r"\bwi ?fi\b", r"\balexa\b", r"\bremote control\b", r"\bled\b",
                 r"\blamp\b", r"\bstring lights?\b", r"\bnight light\b", r"\bplug in\b", r"\bcharg(er|ing)\b",
                 r"\bdigital\b", r"\bsolar\b", r"\bmotion sensor\b", r"\bpowered\b", r"\bcordless\b"],
 "fragile": [r"\bglass\b", r"\bglassware\b", r"\bceramic\b", r"\bporcelain\b", r"\bcrystal\b", r"\bmirror\b",
             r"\bfragile\b", r"\bmarble\b"],
 "ingestible": [r"\bedible\b", r"\bsnacks?\b", r"\bcandy\b", r"\bchocolate\b", r"\bsupplements?\b", r"\bvitamins?\b",
                r"\bgumm(y|ies)\b", r"\bcoffee beans\b", r"\btea bags\b", r"\bbeverage\b", r"\bdrink mix\b"],
 "seasonal": [r"\bchristmas\b", r"\bxmas\b", r"\bhalloween\b", r"\bvalentines?\b", r"\beaster\b",
              r"\bthanksgiving\b", r"\bhanukkah\b", r"\bfall decor\b", r"\bautumn\b", r"\b4th of july\b",
              r"\bst patrick", r"\bseasonal\b"],
 "heavy_furniture": [r"\bfurniture\b", r"\bsofa\b", r"\bcouch\b", r"\bdresser\b", r"\bbookcase\b", r"\bwardrobe\b",
                     r"\bbed frame\b", r"\bdining table\b", r"\bnightstand\b", r"\btv stand\b", r"\bcast iron\b",
                     r"\bconcrete\b", r"\bheavy\b(?! duty)"],
}
CABINET_OK = ["organizer", "rack", "shelf", "liner", "caddy", "pull", "knob", "lock", "basket", "bin"]

WEIGHTS = {"wow_factor": 10, "price": 8, "cogs": 6, "markup": 8, "perceived_value": 8, "pain_point": 8,
           "daily_use": 7, "golden_customer": 6, "lightweight": 5, "safe_shipping": 5, "evergreen": 5,
           "quality": 8, "proven_seller": 8, "content_potential": 8}

STOP = set("the a an and or for with of to in on by from set pack pcs piece pieces x 1 2 3 4 5 6 new".split())

class HuntRequest(BaseModel):
    category: str = "kitchen_utility"
    keyword: Optional[str] = None
    min_price: float = FILTERS["min_price"]
    max_price: float = FILTERS["max_price"]
    max_cogs: float = FILTERS["max_cogs"]
    min_markup: float = FILTERS["min_markup"]
    min_rating: float = FILTERS["min_rating"]
    min_reviews: int = FILTERS["min_reviews"]
    min_score: float = FILTERS["min_score"]

# ---------------------------------------------------------------- database
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE IF NOT EXISTS products (fingerprint TEXT PRIMARY KEY, data TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS hunts (id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT, status TEXT, started_at TEXT, finished_at TEXT, demand_items INTEGER DEFAULT 0, supplier_items INTEGER DEFAULT 0, accepted INTEGER DEFAULT 0, error TEXT)")
    cols = [r[1] for r in c.execute("PRAGMA table_info(hunts)").fetchall()]
    if "diagnostic" not in cols:
        c.execute("ALTER TABLE hunts ADD COLUMN diagnostic TEXT")
    c.execute("CREATE TABLE IF NOT EXISTS hunt_items (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, stage TEXT, data TEXT)")
    c.execute("CREATE INDEX IF NOT EXISTS ix_hunt_items_run ON hunt_items(run_id)")
    c.commit()
    return c

def now(): return datetime.now(timezone.utc).isoformat()

def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()

def fp(name, url=None): return hashlib.sha256((norm(name) + "|" + norm(url or "")).encode()).hexdigest()

def save_product(p):
    c = db(); t = now(); f = fp(p.get("name", ""), p.get("url"))
    old = c.execute("SELECT created_at FROM products WHERE fingerprint=?", (f,)).fetchone()
    p["first_seen"] = old[0] if old else t
    c.execute("INSERT INTO products(fingerprint,data,created_at,updated_at) VALUES(?,?,?,?) "
              "ON CONFLICT(fingerprint) DO UPDATE SET data=excluded.data,updated_at=excluded.updated_at",
              (f, json.dumps(p, ensure_ascii=False), t, t))
    c.commit(); c.close()
    return not bool(old)

def products(limit=50, category=None):
    c = db()
    rows = c.execute("SELECT data FROM products ORDER BY json_extract(data,'$.score') DESC, updated_at DESC LIMIT ?",
                     (max(1, min(limit, 500)),)).fetchall()
    c.close()
    out = [json.loads(r[0]) for r in rows]
    return [p for p in out if not category or p.get("category_key") == category]

# ---------------------------------------------------------------- parsing helpers
def first(d, names, default=None):
    if not isinstance(d, dict): return default
    low = {str(k).lower(): v for k, v in d.items()}
    for n in names:
        v = low.get(n.lower())
        if v not in (None, "", [], {}): return v
    return default

def num(v, default=0.0):
    if isinstance(v, bool): return default
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, dict):
        v = first(v, ["current", "value", "min", "amount", "price", "minPrice", "salePrice", "formatted"], None)
        return num(v, default)
    if isinstance(v, list): return num(v[0], default) if v else default
    if v is None: return default
    s = str