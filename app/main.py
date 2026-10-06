from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Any
from difflib import SequenceMatcher
import os, re, unicodedata, httpx, sqlite3, hashlib, json, asyncio, time
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus

VERSION = "5.6.0"
DB_PATH = os.getenv("PRODUCT_HUNTER_DB", "product_hunter.db")

app = FastAPI(title="Product Hunter API", version=VERSION)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])

CATEGORIES = {
    "kitchen_utility": ["kitchen utility", "kitchen organizer", "kitchen storage", "pantry organizer", "drawer organizer", "countertop organizer", "sink organizer", "kitchen cleaning", "cooking tool", "meal prep", "spice organizer"],
    "home_decor": ["home decor", "home decoration", "wall decor", "table decor", "living room decor", "bedroom decor", "decorative storage", "aesthetic home", "minimalist decor", "home accessory"],
}
WEIGHTS = {"wow_factor":10,"price":8,"cogs":6,"markup":8,"perceived_value":8,"pain_point":8,"daily_use":7,"golden_customer":6,"lightweight":5,"safe_shipping":5,"evergreen":5,"quality":8,"proven_seller":8,"content_potential":8}

AMAZON_DOMAINS = {"US":"amazon.com","UK":"amazon.co.uk","GB":"amazon.co.uk","DE":"amazon.de","FR":"amazon.fr","IT":"amazon.it","ES":"amazon.es","CA":"amazon.ca","AU":"amazon.com.au","IN":"amazon.in","AE":"amazon.ae","SA":"amazon.sa"}
PROXY_COUNTRIES = {"US":"us","UK":"gb","GB":"gb","DE":"de","FR":"fr","IT":"it","ES":"es","CA":"ca","AU":"au","IN":"in","AE":"ae","SA":"sa"}

class HuntRequest(BaseModel):
    category: str = "all"
    keyword: Optional[str] = None
    marketplace: str = "US"
    supplier_ship_to: str = "US"
    currency: str = "USD"
    max_products: int = 20
    amazon_pages: int = 2
    supplier_pages: int = 2
    # Kept for frontend/backward compatibility. Selling price is NEVER used as a rejection filter.
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    max_cogs: float = 35
    min_markup: float = 3
    min_rating: float = 4
    min_reviews: int = 50
    min_score: float = 0


def now():
    return datetime.now(timezone.utc).isoformat()


def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE IF NOT EXISTS products (fingerprint TEXT PRIMARY KEY, data TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS hunts (id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT, status TEXT, started_at TEXT, finished_at TEXT, demand_items INTEGER DEFAULT 0, supplier_items INTEGER DEFAULT 0, accepted INTEGER DEFAULT 0, error TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS product_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, fingerprint TEXT NOT NULL, captured_at TEXT NOT NULL, marketplace TEXT, source TEXT, cumulative_orders REAL, selling_price REAL, cogs REAL, rating REAL, reviews REAL)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_fp_time ON product_snapshots(fingerprint, captured_at)")
    c.commit()
    return c


def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def first(d, names, default=None):
    if not isinstance(d, dict): return default
    low = {str(k).lower(): v for k, v in d.items()}
    for n in names:
        v = low.get(n.lower())
        if v not in (None, "", []): return v
    return default


def num(v, default=0):
    if isinstance(v, (int, float)): return float(v)
    if v is None: return default
    m = re.search(r"-?\d+(?:\.\d+)?", str(v).replace(",", ""))
    return float(m.group()) if m else default


def integer(v, default=0):
    try: return int(num(v, default))
    except Exception: return default


def fp(name, url=None):
    return hashlib.sha256((norm(name) + "|" + norm(url or "")).encode()).hexdigest()


def name_of(x): return str(first(x,["name","title","productName","product_name","productTitle","product_title","itemTitle"],"")).strip()
def price_of(x): return num(first(x,["price","currentPrice","current_price","salePrice","sale_price","sellingPrice","selling_price","priceValue","price_value"],0))
def rating_of(x): return num(first(x,["rating","stars","starRating","star_rating"],0))
def reviews_of(x): return integer(first(x,["reviews","reviewCount","review_count","reviews_count","ratingsTotal","ratings_total"],0))
def orders_of(x): return num(first(x,["orders","orders_count","orderCount","order_count","sold","sold_count","sales","sales_count"],None), 0)
def desc_of(x): return str(first(x,["description","productDescription","product_description","features","bullets","category"],""))
def url_of(x): return first(x,["url","productUrl","product_url","detailPageURL","detailUrl","link"],None)
def image_of(x): return first(x,["image","imageUrl","image_url","thumbnail","thumbnailUrl","mainImage"],None)


def extract_rows(payload):
    if isinstance(payload, list): return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict): return []
    for key in ("products","items","results","data"):
        val = payload.get(key)
        if isinstance(val, list): return [x for x in val if isinstance(x, dict)]
        if isinstance(val, dict):
            nested = extract_rows(val)
            if nested: return nested
    return []


def classify(text):
    t = norm(text)
    scores = {k: sum(1 for kw in kws if norm(kw) in t) for k,kws in CATEGORIES.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else None


def hard_reject(name, desc):
    t = norm(name + " " + desc)
    bad = ["electronic", "battery", "bluetooth", "usb", "smart device", "glass vase", "glassware", "fragile", "edible", "food supplement", "drink", "beverage", "christmas", "halloween", "valentine", "easter"]
    return any(x in t for x in bad)


def sim(a,b):
    aa, bb = set(norm(a).split()), set(norm(b).split())
    overlap = len(aa & bb) / max(1, len(aa | bb))
    return max(SequenceMatcher(None, norm(a), norm(b)).ratio(), overlap)


def score_product(selling, cogs, rating, reviews, name, desc, category, orders=0):
    markup = selling / cogs if cogs else 0
    t = norm(name + " " + desc)
    def s(cond, yes=10, no=4): return yes if cond else no
    parts = {
        "wow_factor": s(len(name) >= 18 and len(set(norm(name).split())) >= 4, 9, 5),
        # Price is a preference signal only; NEVER a rejection rule.
        "price": 10 if 50 <= selling <= 80 else 8 if selling >= 40 else 6,
        "cogs": 10 if 10 <= cogs <= 35 else 7 if cogs > 0 else 5,
        "markup": 10 if markup >= 4 else 9 if markup >= 3 else 6 if markup > 0 else 5,
        "perceived_value": 9 if selling >= 50 else 7 if selling > 0 else 5,
        "pain_point": 9 if any(w in t for w in ["organizer","storage","space","clean","clutter","holder","rack","saving","utility"]) else 6,
        "daily_use": 8 if any(w in t for w in ["kitchen","storage","organizer","clean","holder","drawer","pantry","sink"]) else 6,
        "golden_customer": 8 if category in ("kitchen_utility","home_decor") else 5,
        "lightweight": 9 if not any(w in t for w in ["heavy","large furniture","sofa","cabinet"]) else 2,
        "safe_shipping": 9 if not any(w in t for w in ["glass","ceramic","mirror","crystal"]) else 2,
        "evergreen": 9 if not any(w in t for w in ["christmas","halloween","valentine","easter","seasonal"]) else 2,
        "quality": min(10, max(4, 4 + rating * 1.5)) if rating else 6,
        "proven_seller": 10 if reviews >= 1000 or orders >= 1000 else 9 if reviews >= 500 or orders >= 500 else 8 if reviews >= 50 or orders >= 50 else 6,
        "content_potential": 9 if any(w in t for w in ["organizer","storage","decor","rack","holder","space","clean","before","after"]) else 7,
    }
    total = round(sum(parts[k] * WEIGHTS[k] / 10 for k in WEIGHTS), 1)
    return total, parts


def verdict(s):
    return "EXCEPTIONAL" if s >= 90 else "STRONG WINNER" if s >= 80 else "TEST" if s >= 70 else "WATCH" if s >= 60 else "REJECT"


def save_product(p):
    c = db(); t = now(); f = fp(p.get("name", ""), p.get("url"))
    c.execute("INSERT INTO products(fingerprint,data,created_at,updated_at) VALUES(?,?,?,?) ON CONFLICT(fingerprint) DO UPDATE SET data=excluded.data,updated_at=excluded.updated_at", (f, json.dumps(p, ensure_ascii=False), t, t))
    c.commit(); c.close()
    return f


def products(limit=50, category=None):
    c = db()
    rows = c.execute("SELECT data FROM products ORDER BY json_extract(data,'$.score') DESC, updated_at DESC LIMIT ?", (max(1,min(limit,500)),)).fetchall()
    c.close()
    out = [json.loads(r[0]) for r in rows]
    return [p for p in out if not category or p.get("category_key") == category]


def snapshots_for(fingerprint, limit=50):
    c = db(); rows = c.execute("SELECT * FROM product_snapshots WHERE fingerprint=? ORDER BY captured_at DESC LIMIT ?", (fingerprint, limit)).fetchall(); c.close()
    return [dict(r) for r in rows]


def velocity(fingerprint, current_orders, captured_at):
    if current_orders is None or current_orders <= 0:
        return {"sales_24h":None,"sales_7d":None,"sales_velocity_status":"not_available"}
    c = db(); rows = c.execute("SELECT captured_at,cumulative_orders FROM product_snapshots WHERE fingerprint=? AND cumulative_orders IS NOT NULL ORDER BY captured_at DESC", (fingerprint,)).fetchall(); c.close()
    current = datetime.fromisoformat(captured_at)
    result = {}
    for hours, key in [(24,"sales_24h"),(168,"sales_7d")]:
        target = current - timedelta(hours=hours)
        baseline = None
        for r in rows:
            try:
                ts = datetime.fromisoformat(r[0])
                if ts <= target:
                    baseline = r[1]; break
            except Exception: pass
        result[key] = max(0, round(current_orders - baseline, 2)) if baseline is not None else None
    result["sales_velocity_status"] = "calculated" if result["sales_24h"] is not None or result["sales_7d"] is not None else "collecting_baseline"
    return result


def save_snapshot(fingerprint, p):
    c = db(); ts = p.get("snapshot_at") or now()
    c.execute("INSERT INTO product_snapshots(fingerprint,captured_at,marketplace,source,cumulative_orders,selling_price,cogs,rating,reviews) VALUES(?,?,?,?,?,?,?,?,?)", (fingerprint,ts,p.get("marketplace"),p.get("source"),p.get("cumulative_orders"),p.get("selling_price"),p.get("cost_of_goods"),p.get("rating"),p.get("reviews")))
    c.commit(); c.close()


async def run_actor(actor, token, payload):
    url = f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items"
    async with httpx.AsyncClient(timeout=75) as c:
        r = await c.post(url, params={"token":token,"format":"json","clean":"true"}, json=payload)
        if r.status_code >= 400: raise RuntimeError(f"Apify actor {actor} HTTP {r.status_code}: {r.text[:800]}")
        data = r.json()
        if not isinstance(data, list): raise RuntimeError("Apify returned unexpected data")
        return data


async def run_scrapingant_extract(target_url, extract_properties, proxy_country=None):
    key = os.getenv("SCRAPINGANT_API_KEY")
    if not key: raise RuntimeError("SCRAPINGANT_API_KEY is not configured")
    params = {"url":target_url,"extract_properties":extract_properties,"timeout":25,"browser":"true","x-api-key":key}
    if proxy_country: params["proxy_country"] = proxy_country.lower()
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get("https://api.scrapingant.com/v2/extract", params=params)
        if r.status_code >= 400: raise RuntimeError(f"ScrapingAnt HTTP {r.status_code}: {r.text[:800]}")
        data = r.json()
        return data if isinstance(data, dict) else {}


async def run_scrapingant_amazon(queries, marketplace="US"):
    items=[]; domain=AMAZON_DOMAINS.get((marketplace or "US").upper(),"amazon.com"); country=PROXY_COUNTRIES.get((marketplace or "US").upper(),"us")
    for q in queries[:2]:
        url=f"https://www.{domain}/s?k={quote_plus(q)}"
        data=await run_scrapingant_extract(url,"products(list: title, price(number), rating(number), reviews(number), description, product url, image url, asin)",country)
        items.extend(extract_rows(data))
    return items


async def run_scrapingant_aliexpress(queries, marketplace="US"):
    items=[]; country=PROXY_COUNTRIES.get((marketplace or "US").upper(),"us")
    for q in queries[:2]:
        url=f"https://www.aliexpress.com/wholesale?SearchText={quote_plus(q)}"
        data=await run_scrapingant_extract(url,"products(list: title, price(number), rating(number), orders(number), description, product url, image url, product id)",country)
        items.extend(extract_rows(data))
    return items


async def run_crawlbase(target_url, scraper, marketplace="US"):
    token=os.getenv("CRAWLBASE_TOKEN")
    if not token: raise RuntimeError("CRAWLBASE_TOKEN is not configured")
    params={"token":token,"url":target_url,"scraper":scraper}
    if marketplace: params["country"]=(marketplace or "US").upper()
    async with httpx.AsyncClient(timeout=30) as c:
        r=await c.get("https://api.crawlbase.com/",params=params)
        if r.status_code>=400: raise RuntimeError(f"Crawlbase HTTP {r.status_code}: {r.text[:800]}")
        payload=r.json(); body=payload.get("body") if isinstance(payload,dict) else payload
        if isinstance(body,str):
            try: return json.loads(body)
            except Exception: return {}
        return body if isinstance(body,dict) else {}


async def run_crawlbase_amazon(queries, pages=1, marketplace="US"):
    items=[]; domain=AMAZON_DOMAINS.get((marketplace or "US").upper(),"amazon.com")
    for q in queries[:3]:
        for page in range(1,max(1,min(pages,2))+1):
            data=await run_crawlbase(f"https://www.{domain}/s?k={quote_plus(q)}&page={page}","amazon-serp",marketplace)
            items.extend(extract_rows(data))
    return items


async def run_crawlbase_aliexpress(queries, pages=1, marketplace="US"):
    items=[]
    for q in queries[:3]:
        data=await run_crawlbase(f"https://www.aliexpress.com/wholesale?SearchText={quote_plus(q)}","aliexpress-serp",marketplace)
        items.extend(extract_rows(data))
    return items


async def collect_sources(queries, req):
    demand=[]; supplier=[]; errors=[]; used=[]
    if os.getenv("SCRAPINGANT_API_KEY"):
        try:
            demand=await asyncio.wait_for(run_scrapingant_amazon(queries,req.marketplace),35); used.append("scrapingant_amazon")
        except Exception as e: errors.append(f"scrapingant_amazon: {type(e).__name__}: {e}")
        try:
            supplier=await asyncio.wait_for(run_scrapingant_aliexpress(queries,req.marketplace),35); used.append("scrapingant_aliexpress")
        except Exception as e: errors.append(f"scrapingant_aliexpress: {type(e).__name__}: {e}")
    if not demand and os.getenv("CRAWLBASE_TOKEN"):
        try: demand=await asyncio.wait_for(run_crawlbase_amazon(queries,req.amazon_pages,req.marketplace),35); used.append("crawlbase_amazon")
        except Exception as e: errors.append(f"crawlbase_amazon: {type(e).__name__}: {e}")
    if not supplier and os.getenv("CRAWLBASE_TOKEN"):
        try: supplier=await asyncio.wait_for(run_crawlbase_aliexpress(queries,req.supplier_pages,req.marketplace),35); used.append("crawlbase_aliexpress")
        except Exception as e: errors.append(f"crawlbase_aliexpress: {type(e).__name__}: {e}")
    if (not demand or not supplier) and os.getenv("APIFY_API_TOKEN"):
        token=os.getenv("APIFY_API_TOKEN")
        if not demand and os.getenv("APIFY_DEMAND_ACTOR_ID"):
            try:
                demand=await asyncio.wait_for(run_actor(os.getenv("APIFY_DEMAND_ACTOR_ID"),token,{"searchQueries":queries,"marketplace":req.marketplace,"maxProductsPerSearch":max(5,min(req.max_products,30)),"maxSearchPages":max(1,min(req.amazon_pages,3)),"proxyConfiguration":{"useApifyProxy":True}}),80); used.append("apify_amazon")
            except Exception as e: errors.append(f"apify_amazon: {type(e).__name__}: {e}")
        if not supplier and os.getenv("APIFY_SUPPLIER_ACTOR_ID"):
            try:
                supplier=await asyncio.wait_for(run_actor(os.getenv("APIFY_SUPPLIER_ACTOR_ID"),token,{"mode":"search","searchQuery":" ".join(queries[:2]),"region":"com","currency":req.currency,"language":"en_US","sortBy":"orders_desc","priceMin":0,"priceMax":max(1,int(req.max_cogs)),"shipTo":req.supplier_ship_to,"maxItems":max(20,min(req.max_products*5,80)),"maxPages":max(1,min(req.supplier_pages,5))}),80); used.append("apify_aliexpress")
            except Exception as e: errors.append(f"apify_aliexpress: {type(e).__name__}: {e}")
    return demand,supplier,used,errors


async def execute_hunt(category, req):
    if category not in CATEGORIES: raise ValueError("Only kitchen_utility and home_decor are allowed")
    queries=[req.keyword] if req.keyword else CATEGORIES[category][:5]
    queries=[q for q in queries if q]
    c=db(); run_id=c.execute("INSERT INTO hunts(category,status,started_at) VALUES(?,?,?)",(category,"running",now())).lastrowid; c.commit(); c.close()
    try:
        demand,supplier,used,source_errors=await collect_sources(queries,req)
        suppliers=[]
        for x in supplier:
            n=name_of(x); cost=price_of(x)
            if n and cost>0: suppliers.append((n,cost,x))
        candidates=[]
        for i,x in enumerate(demand):
            n=name_of(x); sell=price_of(x); desc=desc_of(x); rating=rating_of(x); reviews=reviews_of(x); orders=orders_of(x)
            if not n or sell<=0 or hard_reject(n,desc): continue
            cat=classify(n+" "+desc)
            if cat and cat!=category: continue
            # IMPORTANT: no selling-price filter; no rating/review hard filter.
            best=None; bestsim=0
            for sn,sc,sx in suppliers:
                z=sim(n,sn)
                if z>bestsim: bestsim=z; best=(sn,sc,sx)
            supplier_status="matched" if best and bestsim>=0.38 else "pending"
            supplier_name=cogs=sx=None
            if best and bestsim>=0.38: supplier_name,cogs,sx=best
            score,breakdown=score_product(sell,cogs or 0,rating,reviews,n,desc,category,orders)
            if supplier_status=="matched" and cogs>0 and sell/cogs>=req.min_markup and cogs<=req.max_cogs:
                winner_score=score; verdict_name=verdict(score)
            else:
                # Demand-only candidates stay visible; they are not rejected just because supplier data is missing.
                winner_score=score; verdict_name="CANDIDATE"
            ts=now(); fingerprint=fp(n,url_of(x)); vel=velocity(fingerprint,orders if orders>0 else None,ts)
            p={"id":str(first(x,["asin","id","sku","productId","product_id"],f"product-{i}")),"fingerprint":fingerprint,"name":n,"category_key":category,"category":"Kitchen Utility" if category=="kitchen_utility" else "Home Decor","marketplace":req.marketplace,"selling_price":round(sell,2),"cost_of_goods":round(cogs,2) if cogs else None,"markup":round(sell/cogs,2) if cogs else None,"rating":rating,"reviews":reviews,"cumulative_orders":orders if orders>0 else None,"sales_24h":vel["sales_24h"],"sales_7d":vel["sales_7d"],"sales_velocity_status":vel["sales_velocity_status"],"score":score,"winner_score":winner_score,"verdict":verdict_name,"supplier_status":supplier_status,"supplier_name":supplier_name,"supplier_match_score":round(bestsim,3) if bestsim else 0,"image":image_of(x),"url":url_of(x),"supplier_url":url_of(sx) if sx else None,"breakdown":breakdown,"source":used[0] if used else "unknown","sources_used":used,"discovered_at":ts,"snapshot_at":ts}
            save_product(p); save_snapshot(fingerprint,p); candidates.append(p)
        c=db(); c.execute("UPDATE hunts SET status=?,finished_at=?,demand_items=?,supplier_items=?,accepted=?,error=? WHERE id=?",("finished",now(),len(demand),len(supplier),len(candidates),"; ".join(source_errors)[:3000] if source_errors else None,run_id)); c.commit(); c.close()
        return {"status":"success","category":category,"run_id":run_id,"demand_items":len(demand),"supplier_items":len(supplier),"accepted":len(candidates),"source_errors":source_errors,"sources_used":used,"products":sorted(candidates,key=lambda p:p["score"],reverse=True)}
    except Exception as e:
        c=db(); c.execute("UPDATE hunts SET status=?,finished_at=?,error=? WHERE id=?",("error",now(),str(e)[:3000],run_id)); c.commit(); c.close(); raise


@app.on_event("startup")
def startup(): db().close()

@app.get("/health")
def health():
    db().close(); return {"status":"ok","version":VERSION,"database":"connected","scrapingant_configured":bool(os.getenv("SCRAPINGANT_API_KEY")),"crawlbase_configured":bool(os.getenv("CRAWLBASE_TOKEN")),"apify_configured":bool(os.getenv("APIFY_API_TOKEN")),"categories_available":list(CATEGORIES)}

@app.get("/sources/status")
def sources_status():
    return {"scrapingant":bool(os.getenv("SCRAPINGANT_API_KEY")),"crawlbase":bool(os.getenv("CRAWLBASE_TOKEN")),"apify":bool(os.getenv("APIFY_API_TOKEN")),"demand_actor":bool(os.getenv("APIFY_DEMAND_ACTOR_ID")),"supplier_actor":bool(os.getenv("APIFY_SUPPLIER_ACTOR_ID"))}

@app.get("/debug/sources")
async def debug_sources():
    result={"version":VERSION,"scrapingant":{"status":"not_tested"},"crawlbase":{"status":"not_tested"}}
    if os.getenv("SCRAPINGANT_API_KEY"):
        for label,fn in [("amazon",lambda:run_scrapingant_amazon(["kitchen organizer"],"US")),("aliexpress",lambda:run_scrapingant_aliexpress(["kitchen organizer"],"US"))]:
            try:
                rows=await asyncio.wait_for(fn(),35); result["scrapingant"][label]={"status":"ok" if rows else "empty","items":len(rows)}
            except Exception as e: result["scrapingant"][label]={"status":"error","error":f"{type(e).__name__}: {e}"}
    else: result["scrapingant"]={"status":"not_configured"}
    if os.getenv("CRAWLBASE_TOKEN"):
        for label,fn in [("amazon",lambda:run_crawlbase_amazon(["kitchen organizer"],1,"US")),("aliexpress",lambda:run_crawlbase_aliexpress(["kitchen organizer"],1,"US"))]:
            try:
                rows=await asyncio.wait_for(fn(),35); result["crawlbase"][label]={"status":"ok" if rows else "empty","items":len(rows)}
            except Exception as e: result["crawlbase"][label]={"status":"error","error":f"{type(e).__name__}: {e}"}
    else: result["crawlbase"]={"status":"not_configured"}
    return result

@app.get("/products/candidates")
def candidates(limit:int=100,category:Optional[str]=None):
    ps=products(limit,category); return {"status":"success","count":len(ps),"products":ps}

@app.get("/products/accepted")
def accepted(limit:int=100,category:Optional[str]=None):
    ps=products(limit,category); return {"status":"success","count":len(ps),"products":ps}

@app.get("/products/winners")
def winners(limit:int=50):
    ps=[p for p in products(500) if p.get("supplier_status")=="matched" and p.get("markup") is not None and p.get("markup",0)>=3 and p.get("score",0)>=70]
    return {"status":"success","count":min(limit,len(ps)),"products":ps[:limit]}

@app.get("/products/category/{category}")
def by_category(category:str,limit:int=100):
    if category not in CATEGORIES: return {"status":"error","message":"Only kitchen_utility and home_decor are available"}
    return accepted(limit,category)

@app.get("/products/{product_id}")
def product_detail(product_id:str):
    for p in products(500):
        if str(p.get("id"))==product_id or p.get("fingerprint")==product_id or fp(p.get("name",""),p.get("url"))==product_id:
            p["snapshots"]=snapshots_for(p.get("fingerprint"),50)
            return {"status":"success","product":p}
    return {"status":"error","message":"Product not found"}

@app.get("/pipeline/latest")
def pipeline_latest():
    c=db(); rows=c.execute("SELECT * FROM hunts ORDER BY id DESC LIMIT 20").fetchall(); cutoff=time.time()-300
    for r in rows:
        if r["status"]=="running":
            try:
                if datetime.fromisoformat(r["started_at"]).timestamp()<cutoff: c.execute("UPDATE hunts SET status=?,finished_at=?,error=? WHERE id=? AND status='running'",("timeout",now(),"Run exceeded 5-minute watchdog limit",r["id"]))
            except Exception: pass
    c.commit(); rows=c.execute("SELECT * FROM hunts ORDER BY id DESC LIMIT 20").fetchall(); c.close(); runs=[dict(r) for r in rows]
    return {"status":"success","latest":runs[0] if runs else None,"runs":runs,"stages":{"demand_collection":"configured","supplier_collection":"configured","matching":"configured","scoring":"configured","sales_snapshots":"configured","price_filter":"disabled"}}

@app.get("/scraper-data/latest")
def scraper_data_latest(limit:int=100):
    ps=products(limit); return {"status":"success","source":"scrapingant+crawlbase+apify","demand_items":len(ps),"supplier_items":sum(1 for p in ps if p.get("supplier_status")=="matched"),"products":ps}

@app.get("/stats")
def stats():
    ps=products(500); return {"products":len(ps),"exceptional":sum(p.get("score",0)>=90 for p in ps),"strong_winner":sum(80<=p.get("score",0)<90 for p in ps),"test":sum(70<=p.get("score",0)<80 for p in ps),"watch":sum(60<=p.get("score",0)<70 for p in ps),"candidates":sum(p.get("verdict")=="CANDIDATE" for p in ps),"kitchen_utility":sum(p.get("category_key")=="kitchen_utility" for p in ps),"home_decor":sum(p.get("category_key")=="home_decor" for p in ps)}

@app.get("/hunt-history")
def history(limit:int=20):
    c=db(); rows=[dict(r) for r in c.execute("SELECT * FROM hunts ORDER BY id DESC LIMIT ?",(max(1,min(limit,100)),)).fetchall()]; c.close(); return {"runs":rows}

@app.get("/verdicts")
def verdicts():
    ps=products(500); return {v:sum(p.get("verdict")==v for p in ps) for v in ["EXCEPTIONAL","STRONG WINNER","TEST","WATCH","CANDIDATE"]}

@app.post("/auto-hunt/{category}")
async def auto_hunt(category:str, background_tasks:BackgroundTasks):
    if category not in CATEGORIES and category!="all": return {"status":"error","message":"Category must be kitchen_utility, home_decor or all"}
    cats=list(CATEGORIES) if category=="all" else [category]
    for cat in cats: background_tasks.add_task(_bg_hunt,cat)
    return {"status":"success","message":"Hunt started","categories":cats}

async def _bg_hunt(cat):
    try: await asyncio.wait_for(execute_hunt(cat,HuntRequest(category=cat,max_products=20,min_score=0)),150)
    except Exception as e: print(f"Hunt {cat} failed: {e}")

@app.post("/admin/hunt-now/{category}")
async def hunt_now(category:str):
    if category not in CATEGORIES: return {"status":"error","message":"Invalid category"}
    try: return await asyncio.wait_for(execute_hunt(category,HuntRequest(category=category,max_products=20,min_score=0)),150)
    except Exception as e: return {"status":"error","message":str(e)}

@app.post("/hunt")
async def legacy_hunt(req:HuntRequest):
    cat=req.category if req.category in CATEGORIES else classify(req.keyword or "")
    if cat not in CATEGORIES: cat="kitchen_utility"
    try: return await asyncio.wait_for(execute_hunt(cat,req),150)
    except Exception as e: return {"status":"error","message":str(e)}

@app.get("/")
def root():
    return {"service":"Product Hunter API","version":VERSION,"architecture":"ScrapingAnt -> Crawlbase -> Apify; no price rejection; candidate + winner pipeline","sources":["scrapingant","crawlbase","apify"],"categories":list(CATEGORIES),"endpoints":["/health","/debug/sources","/auto-hunt/all","/products/candidates","/products/accepted","/products/winners","/products/{product_id}","/pipeline/latest","/stats"]}
