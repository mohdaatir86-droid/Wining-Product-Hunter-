from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Any
from difflib import SequenceMatcher
import os, re, unicodedata, httpx, sqlite3, hashlib, json, asyncio, time
from datetime import datetime, timezone
from urllib.parse import quote_plus

VERSION="5.4.4"
DB_PATH=os.getenv("PRODUCT_HUNTER_DB","product_hunter.db")

app=FastAPI(title="Product Hunter API", version=VERSION)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])

CATEGORIES={
 "kitchen_utility":["kitchen","kitchen utility","organizer","storage","pantry","drawer","countertop","sink","spice","cooking","meal prep","dish","utensil","cabinet","shelf","rack","holder","chopping","cutting","cleaning","baking"],
 "home_decor":["home decor","home decoration","decor","decorative","wall","table decor","living room","bedroom","aesthetic","minimalist","home accessory","art","poster","frame","cushion","throw","shelf","vase","planter","ornament","sculpture","accent"]
}
WEIGHTS={"wow_factor":10,"price":8,"cogs":6,"markup":8,"perceived_value":8,"pain_point":8,"daily_use":7,"golden_customer":6,"lightweight":5,"safe_shipping":5,"evergreen":5,"quality":8,"proven_seller":8,"content_potential":8}

class HuntRequest(BaseModel):
    category:str="all"
    keyword:Optional[str]=None
    marketplace:str="US"
    supplier_ship_to:str="US"
    currency:str="USD"
    max_products:int=20
    amazon_pages:int=2
    supplier_pages:int=2
    min_price:float=40
    max_price:float=150
    max_cogs:float=35
    min_markup:float=3
    min_rating:float=4
    min_reviews:int=50
    min_score:float=60


def db():
    c=sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory=sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE IF NOT EXISTS products (fingerprint TEXT PRIMARY KEY, data TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS hunts (id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT, status TEXT, started_at TEXT, finished_at TEXT, demand_items INTEGER DEFAULT 0, supplier_items INTEGER DEFAULT 0, accepted INTEGER DEFAULT 0, error TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS pipeline (run_id INTEGER PRIMARY KEY, category TEXT, stage TEXT, message TEXT, seen INTEGER DEFAULT 0, suppliers INTEGER DEFAULT 0, accepted INTEGER DEFAULT 0, updated_at TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS scraper_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, source TEXT, fetched_at TEXT, item_count INTEGER, sample_json TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS hunt_diagnostics (run_id INTEGER PRIMARY KEY, diagnostics_json TEXT, updated_at TEXT)")
    c.commit(); return c

def now(): return datetime.now(timezone.utc).isoformat()

def fp(name,url=None): return hashlib.sha256((norm(name)+"|"+norm(url or "")).encode()).hexdigest()

def save_product(p):
    c=db(); t=now(); f=fp(p.get("name",""),p.get("url"))
    c.execute("INSERT INTO products(fingerprint,data,created_at,updated_at) VALUES(?,?,?,?) ON CONFLICT(fingerprint) DO UPDATE SET data=excluded.data,updated_at=excluded.updated_at",(f,json.dumps(p,ensure_ascii=False),t,t)); c.commit(); c.close()

def products(limit=50,category=None):
    c=db(); rows=c.execute("SELECT data FROM products ORDER BY json_extract(data,'$.score') DESC, updated_at DESC LIMIT ?",(max(1,min(limit,200)),)).fetchall(); c.close()
    out=[json.loads(r[0]) for r in rows]
    return [p for p in out if not category or p.get("category_key")==category]

def first(d,names,default=None):
    if not isinstance(d,dict): return default
    low={str(k).lower():v for k,v in d.items()}
    for n in names:
        v=low.get(n.lower())
        if v not in (None,""): return v
    return default

def num(v,default=0):
    if isinstance(v,(int,float)): return float(v)
    if v is None:return default
    m=re.search(r"-?\d+(?:\.\d+)?",str(v).replace(",",""))
    return float(m.group()) if m else default

def integer(v,default=0):
    try:return int(num(v,default))
    except:return default

def norm(s):
    s=unicodedata.normalize("NFKD",str(s)).encode("ascii","ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+"," ",s).strip()

def name_of(x): return str(first(x,["name","title","productName","product_name","productTitle","product_title","itemTitle"],"")).strip()
def price_of(x):
    v = first(x,["price","currentPrice","current_price","salePrice","sale_price","sellingPrice","selling_price","priceValue","price_value"],0)
    # Crawlbase may return "$49.99", "49.99 USD", or a numeric value.
    return num(v)

def rating_of(x):
    v = first(x,["rating","stars","starRating","star_rating","ratingValue"],0)
    r = num(v)
    return r if 0 <= r <= 5 else 0

def reviews_of(x):
    v = first(x,["reviews","reviews_count","reviewCount","review_count","ratingsTotal","ratings_total","orders","orders_count","orderCount","order_count"],0)
    return integer(v)
def desc_of(x): return str(first(x,["description","productDescription","product_description","features","bullets","category"],""))

def classify(text):
    t=norm(text)
    kitchen_terms=["kitchen","pantry","spice","utensil","dish","sink","drawer","cabinet","countertop","cooking","baking","meal prep","organizer","storage","rack","holder","shelf","chopping","cutting","cleaning"]
    decor_terms=["decor","decorative","wall","table","living room","bedroom","aesthetic","minimalist","poster","frame","cushion","throw","vase","planter","ornament","sculpture","accent","art"]
    ks=sum(2 if term in t else 0 for term in kitchen_terms)
    ds=sum(2 if term in t else 0 for term in decor_terms)
    if ks==0 and ds==0:
        return None
    return "kitchen_utility" if ks>=ds else "home_decor"

def hard_reject(name,desc):
    t=norm(name+" "+desc)
    bad=["electronic","battery","bluetooth","usb","smart device","glass vase","glassware","fragile","edible","food supplement","drink","beverage","seasonal","christmas","halloween","valentine","easter"]
    return any(x in t for x in bad)

def sim(a,b):
    aa=set(norm(a).split()); bb=set(norm(b).split())
    overlap=len(aa&bb)/max(1,len(aa|bb))
    return max(SequenceMatcher(None,norm(a),norm(b)).ratio(),overlap)

def score_product(selling,cogs,rating,reviews,name,desc,category):
    markup=selling/cogs if cogs else 0
    t=norm(name+" "+desc)
    def s(cond,yes=10,no=4): return yes if cond else no
    parts={
      "wow_factor": s(len(name)>=18 and len(set(norm(name).split()))>=4,9,5),
      "price": 10 if 50<=selling<=80 else 8 if selling>=40 else 0,
      "cogs": 10 if 10<=cogs<=35 else 7 if cogs>0 else 0,
      "markup": 10 if markup>=4 else 9 if markup>=3 else 0,
      "perceived_value": 9 if selling>=50 else 7,
      "pain_point": 8 if any(w in t for w in ["organizer","storage","space","clean","clutter","holder","rack","saving","utility"]) else 6,
      "daily_use": 8 if any(w in t for w in ["kitchen","storage","organizer","clean","holder","drawer","pantry","sink"]) else 6,
      "golden_customer": 8 if category in ("kitchen_utility","home_decor") else 5,
      "lightweight": 9 if not any(w in t for w in ["heavy","large furniture","sofa","cabinet"]) else 2,
      "safe_shipping": 9 if not any(w in t for w in ["glass","ceramic","mirror","crystal"]) else 2,
      "evergreen": 9 if not any(w in t for w in ["christmas","halloween","valentine","easter","seasonal"]) else 2,
      "quality": min(10,max(4,4+rating*1.5)),
      "proven_seller": 10 if reviews>=1000 else 9 if reviews>=500 else 8 if reviews>=50 else 0,
      "content_potential": 9 if any(w in t for w in ["organizer","storage","decor","rack","holder","space","clean","before","after"]) else 7,
    }
    total=round(sum(parts[k]*WEIGHTS[k]/10 for k in WEIGHTS),1)
    return total,parts

def verdict(s):
    return "EXCEPTIONAL" if s>=90 else "STRONG WINNER" if s>=80 else "TEST" if s>=70 else "WATCH" if s>=60 else "REJECT"

async def run_actor(actor,token,payload):
    url=f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items"
    async with httpx.AsyncClient(timeout=360) as c:
        r=await c.post(url,params={"token":token,"format":"json","clean":"true"},json=payload)
        if r.status_code>=400: raise RuntimeError(f"Apify actor {actor} HTTP {r.status_code}: {r.text[:1000]}")
        data=r.json()
        if not isinstance(data,list): raise RuntimeError(f"Apify actor {actor} returned unexpected data")
        return data


async def run_crawlbase(target_url, scraper):
    """Call Crawlbase Crawling API and return the scraper's structured JSON."""
    token = os.getenv("CRAWLBASE_TOKEN")
    if not token:
        raise RuntimeError("CRAWLBASE_TOKEN is not configured")
    params = {
        "token": token,
        "url": target_url,
        "scraper": scraper,
    }
    async with httpx.AsyncClient(timeout=120) as c:
        r = await c.get("https://api.crawlbase.com/", params=params)
        if r.status_code >= 400:
            raise RuntimeError(f"Crawlbase HTTP {r.status_code}: {r.text[:1000]}")
        payload = r.json()
        body = payload.get("body") if isinstance(payload, dict) else payload
        if isinstance(body, str):
            try:
                return json.loads(body)
            except Exception:
                return {}
        return body if isinstance(body, dict) else {}

async def run_crawlbase_amazon(queries, pages=1):
    items = []
    for q in queries[:5]:
        for page in range(1, max(1, min(pages, 3)) + 1):
            url = f"https://www.amazon.com/s?k={quote_plus(q)}&page={page}"
            data = await run_crawlbase(url, "amazon-serp")
            rows = data.get("products", []) if isinstance(data, dict) else []
            for x in rows if isinstance(rows, list) else []:
                if isinstance(x, dict):
                    y = dict(x)
                    if "title" in y and "name" not in y:
                        y["name"] = y["title"]
                    if "reviews_count" in y and "reviews" not in y:
                        y["reviews"] = y["reviews_count"]
                    items.append(y)
    return items

async def run_crawlbase_aliexpress(queries, pages=1):
    items = []
    for q in queries[:5]:
        # AliExpress SERP accepts search/wholesale URLs and returns structured products.
        url = f"https://www.aliexpress.com/wholesale?SearchText={quote_plus(q)}"
        data = await run_crawlbase(url, "aliexpress-serp")
        rows = data.get("products", []) if isinstance(data, dict) else []
        for x in rows if isinstance(rows, list) else []:
            if isinstance(x, dict):
                y = dict(x)
                if "title" in y and "name" not in y:
                    y["name"] = y["title"]
                if "orders_count" in y and "orders" not in y:
                    y["orders"] = y["orders_count"]
                items.append(y)
    return items

def set_pipeline(run_id, category, stage, message, seen=0, suppliers=0, accepted=0):
    c=db(); c.execute("INSERT INTO pipeline(run_id,category,stage,message,seen,suppliers,accepted,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET stage=excluded.stage,message=excluded.message,seen=excluded.seen,suppliers=excluded.suppliers,accepted=excluded.accepted,updated_at=excluded.updated_at",(run_id,category,stage,message,seen,suppliers,accepted,now())); c.commit(); c.close()

def save_scraper_snapshot(run_id, source, items):
    sample = items[:25] if isinstance(items,list) else []
    c=db(); c.execute("INSERT INTO scraper_snapshots(run_id,source,fetched_at,item_count,sample_json) VALUES(?,?,?,?,?)",(run_id,source,now(),len(items) if isinstance(items,list) else 0,json.dumps(sample,ensure_ascii=False,default=str))); c.commit(); c.close()

def save_diagnostics(run_id, diagnostics):
    c=db(); c.execute("INSERT INTO hunt_diagnostics(run_id,diagnostics_json,updated_at) VALUES(?,?,?) ON CONFLICT(run_id) DO UPDATE SET diagnostics_json=excluded.diagnostics_json,updated_at=excluded.updated_at",(run_id,json.dumps(diagnostics,ensure_ascii=False),now())); c.commit(); c.close()

async def execute_hunt(category, req):
    if category not in CATEGORIES:
        raise ValueError("Only kitchen_utility and home_decor are allowed")

    queries = [req.keyword] if req.keyword else CATEGORIES[category][:5]
    queries = [q for q in queries if q]

    started = now()
    c = db()
    cur = c.execute(
        "INSERT INTO hunts(category,status,started_at) VALUES(?,?,?)",
        (category, "running", started),
    )
    run_id = cur.lastrowid
    c.commit()
    c.close()
    set_pipeline(run_id, category, "starting", "Preparing product hunt")

    demand = []
    supplier = []
    source_errors = []

    try:
        # Crawlbase is now the primary source.
        if os.getenv("CRAWLBASE_TOKEN"):
            try:
                set_pipeline(run_id, category, "scraping_amazon", "Fetching Amazon demand data")
                demand = await run_crawlbase_amazon(queries, req.amazon_pages)
                save_scraper_snapshot(run_id, "amazon", demand)
                set_pipeline(run_id, category, "scraping_amazon_done", f"Amazon returned {len(demand)} records", seen=len(demand))
            except Exception as e:
                source_errors.append(f"crawlbase_amazon: {e}")

            try:
                set_pipeline(run_id, category, "scraping_aliexpress", "Fetching AliExpress supplier data")
                supplier = await run_crawlbase_aliexpress(queries, req.supplier_pages)
                save_scraper_snapshot(run_id, "aliexpress", supplier)
                set_pipeline(run_id, category, "scraping_aliexpress_done", f"AliExpress returned {len(supplier)} records", seen=len(demand), suppliers=len(supplier))
            except Exception as e:
                source_errors.append(f"crawlbase_aliexpress: {e}")

        # Optional fallback: Apify only if Crawlbase produced no data.
        if (not demand or not supplier) and os.getenv("APIFY_API_TOKEN"):
            token = os.getenv("APIFY_API_TOKEN")
            demand_actor = os.getenv("APIFY_DEMAND_ACTOR_ID")
            supplier_actor = os.getenv("APIFY_SUPPLIER_ACTOR_ID")

            if not demand and demand_actor:
                try:
                    demand_input = {
                        "searchQueries": queries,
                        "marketplace": req.marketplace,
                        "maxProductsPerSearch": max(5, min(req.max_products, 50)),
                        "maxSearchPages": max(1, min(req.amazon_pages, 5)),
                        "proxyConfiguration": {
                            "useApifyProxy": True,
                            "apifyProxyGroups": ["RESIDENTIAL"],
                        },
                    }
                    demand = await run_actor(demand_actor, token, demand_input)
                except Exception as e:
                    source_errors.append(f"apify_amazon: {e}")

            if not supplier and supplier_actor:
                try:
                    supplier_input = {
                        "mode": "search",
                        "searchQuery": " ".join(queries[:2]),
                        "region": "com",
                        "currency": "USD",
                        "language": "en_US",
                        "sortBy": "orders_desc",
                        "priceMin": 0,
                        "priceMax": max(1, int(req.max_cogs)),
                        "ratingMin": req.min_rating,
                        "shipTo": req.supplier_ship_to,
                        "maxItems": max(20, min(req.max_products * 5, 100)),
                        "maxPages": max(1, min(req.supplier_pages, 10)),
                        "enrichReviews": False,
                        "useProxy": False,
                        "autoEscalateOnBlock": True,
                    }
                    supplier = await run_actor(supplier_actor, token, supplier_input)
                except Exception as e:
                    source_errors.append(f"apify_aliexpress: {e}")

        set_pipeline(run_id, category, "normalizing", f"Normalizing {len(demand)} demand and {len(supplier)} supplier records", seen=len(demand), suppliers=len(supplier))
        suppliers = []
        for x in supplier:
            n = name_of(x)
            cost = price_of(x)
            if n and cost > 0:
                suppliers.append((n, cost, x))

        accepted = []
        rejected = 0
        diagnostics = {
            "seen": len(demand),
            "rejected_hard_or_empty": 0,
            "rejected_category": 0,
            "rejected_price": 0,
            "rejected_rating_reviews": 0,
            "rejected_supplier_match": 0,
            "rejected_cogs_markup": 0,
            "rejected_score": 0,
        }

        set_pipeline(run_id, category, "filtering", "Applying category, price, rating, reviews and shipping-safety rules", seen=len(demand), suppliers=len(supplier))
        for i, x in enumerate(demand):
            n = name_of(x)
            sell = price_of(x)
            rating = rating_of(x)
            reviews = reviews_of(x)
            desc = desc_of(x)

            if not n or sell <= 0 or hard_reject(n, desc):
                rejected += 1
                diagnostics["rejected_hard_or_empty"] += 1
                continue

            cat = classify(n + " " + desc)
            # The hunt query already defines the target category. Only reject when
            # the product is confidently classified into the OTHER category.
            if cat is not None and cat != category:
                rejected += 1
                diagnostics["rejected_category"] += 1
                continue

            if sell < req.min_price or sell > req.max_price:
                rejected += 1
                diagnostics["rejected_price"] += 1
                continue
            if rating < req.min_rating or reviews < req.min_reviews:
                rejected += 1
                diagnostics["rejected_rating_reviews"] += 1
                continue

            best = None
            bestsim = 0
            for sn, sc, sx in suppliers:
                z = sim(n, sn)
                if z > bestsim:
                    bestsim = z
                    best = (sn, sc, sx)

            # Marketplace titles often differ substantially. The supplier
            # catalogue is already category/query constrained, so use the best
            # available supplier candidate and let COGS + markup validate it.
            if not best:
                rejected += 1
                diagnostics["rejected_supplier_match"] += 1
                continue

            supplier_name, cogs, sx = best
            if cogs <= 0 or cogs > req.max_cogs or sell / cogs < req.min_markup:
                rejected += 1
                diagnostics["rejected_cogs_markup"] += 1
                continue

            if i % 10 == 0:
                set_pipeline(run_id, category, "scoring", f"Scoring demand record {i+1}/{len(demand)}", seen=len(demand), suppliers=len(supplier), accepted=len(accepted))
            score, breakdown = score_product(
                sell, cogs, rating, reviews, n, desc, category
            )
            if score < req.min_score:
                rejected += 1
                diagnostics["rejected_score"] += 1
                continue

            p = {
                "id": str(first(x, ["asin", "id", "sku", "productId"], f"amazon-{i}")),
                "name": n,
                "category_key": category,
                "category": "Kitchen Utility" if category == "kitchen_utility" else "Home Décor",
                "selling_price": round(sell, 2),
                "cost_of_goods": round(cogs, 2),
                "markup": round(sell / cogs, 2),
                "rating": rating,
                "reviews": reviews,
                "score": score,
                "winner_score": score,
                "verdict": verdict(score),
                "supplier_name": supplier_name,
                "supplier_match_score": round(bestsim, 3),
                "image": first(x, ["image", "imageUrl", "image_url", "thumbnail", "thumbnailUrl"], None),
                "url": first(x, ["url", "productUrl", "product_url", "detailPageURL"], None),
                "supplier_url": first(sx, ["url", "productUrl", "product_url", "detailUrl"], None),
                "breakdown": breakdown,
                "discovered_at": now(),
                "source": "crawlbase",
            }
            save_product(p)
            accepted.append(p)

        save_diagnostics(run_id, diagnostics)
        set_pipeline(run_id, category, "complete", f"Completed: {len(accepted)} accepted from {len(demand)} demand records", seen=len(demand), suppliers=len(supplier), accepted=len(accepted))
        status = "success" if demand or supplier else "success"
        error_text = "; ".join(source_errors)[:2000] if source_errors else None

        c = db()
        c.execute(
            "UPDATE hunts SET status=?,finished_at=?,demand_items=?,supplier_items=?,accepted=?,error=? WHERE id=?",
            (status, now(), len(demand), len(supplier), len(accepted), error_text, run_id),
        )
        c.commit()
        c.close()

        return {
            "status": "success",
            "category": category,
            "run_id": run_id,
            "demand_items": len(demand),
            "supplier_items": len(supplier),
            "accepted": len(accepted),
            "rejected": rejected,
            "source_errors": source_errors,
            "diagnostics": diagnostics,
            "products": sorted(accepted, key=lambda p: p["score"], reverse=True),
        }

    except Exception as e:
        set_pipeline(run_id, category, "error", str(e)[:300], seen=len(demand), suppliers=len(supplier), accepted=0)
        c = db()
        c.execute(
            "UPDATE hunts SET status=?,finished_at=?,error=? WHERE id=?",
            ("error", now(), str(e)[:2000], run_id),
        )
        c.commit()
        c.close()
        raise

@app.on_event("startup")
def startup(): db().close()

@app.get("/health")
def health():
    db().close()
    return {"status":"ok","version":VERSION,"database":"connected","crawlbase_configured":bool(os.getenv("CRAWLBASE_TOKEN")),"apify_configured":bool(os.getenv("APIFY_API_TOKEN")),"categories_available":list(CATEGORIES.keys()),"transparency_endpoints":["/pipeline/latest","/scraper-data/latest"]}

@app.get("/sources/status")
def sources(): return {"crawlbase":bool(os.getenv("CRAWLBASE_TOKEN")),"apify":bool(os.getenv("APIFY_API_TOKEN")),"demand_actor":bool(os.getenv("APIFY_DEMAND_ACTOR_ID")),"supplier_actor":bool(os.getenv("APIFY_SUPPLIER_ACTOR_ID"))}

@app.get("/diagnostic/latest")
def diagnostic_latest():
    c=db()
    row=c.execute("SELECT category,status,demand_items,supplier_items,accepted,error,finished_at FROM hunts ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    if not row:
        return {"status":"success","message":"No hunts yet"}
    return {
        "status":"success",
        "category":row[0],
        "hunt_status":row[1],
        "seen":row[2],
        "suppliers":row[3],
        "accepted":row[4],
        "error":row[5],
        "finished_at":row[6],
        "note":"v5.4.3 uses targeted discovery queries and no hard supplier-title similarity gate; COGS/markup remains enforced."
    }

@app.get("/pipeline/latest")
def pipeline_latest():
    c=db(); row=c.execute("SELECT run_id,category,stage,message,seen,suppliers,accepted,updated_at FROM pipeline ORDER BY run_id DESC LIMIT 1").fetchone(); diagrow=None if not row else c.execute("SELECT diagnostics_json FROM hunt_diagnostics WHERE run_id=?",(row[0],)).fetchone(); c.close()
    if not row: return {"status":"success","stage":"idle","message":"No hunt has run yet","seen":0,"suppliers":0,"accepted":0,"run_id":None,"diagnostics":{}}
    try: diagnostics=json.loads(diagrow[0]) if diagrow else {}
    except: diagnostics={}
    return {"status":"success","run_id":row[0],"category":row[1],"stage":row[2],"message":row[3],"seen":row[4],"suppliers":row[5],"accepted":row[6],"updated_at":row[7],"diagnostics":diagnostics}

@app.get("/scraper-data/latest")
def scraper_data_latest(limit:int=25):
    c=db(); rows=c.execute("SELECT run_id,source,fetched_at,item_count,sample_json FROM scraper_snapshots ORDER BY id DESC LIMIT 2").fetchall(); c.close()
    out=[]
    for r in rows:
        try: sample=json.loads(r[4])
        except: sample=[]
        out.append({"run_id":r[0],"source":r[1],"fetched_at":r[2],"item_count":r[3],"sample":sample[:max(1,min(limit,50))]})
    return {"status":"success","snapshots":out}

@app.get("/products/accepted")
def accepted(limit:int=50,category:Optional[str]=None): return {"status":"success","count":len(products(limit,category)),"products":products(limit,category)}

@app.get("/products/winners")
def winners(limit:int=50): return {"status":"success","products":[p for p in products(limit) if p.get("score",0)>=80]}

@app.get("/products/category/{category}")
def by_category(category:str,limit:int=50):
    if category not in CATEGORIES: return {"status":"error","message":"Only kitchen_utility and home_decor are available"}
    return accepted(limit,category)

@app.get("/stats")
def stats():
    ps=products(200); return {"products":len(ps),"exceptional":sum(p.get("score",0)>=90 for p in ps),"strong_winner":sum(80<=p.get("score",0)<90 for p in ps),"test":sum(70<=p.get("score",0)<80 for p in ps),"watch":sum(60<=p.get("score",0)<70 for p in ps),"kitchen_utility":sum(p.get("category_key")=="kitchen_utility" for p in ps),"home_decor":sum(p.get("category_key")=="home_decor" for p in ps)}

@app.get("/hunt-history")
def history(limit:int=20):
    c=db(); rows=[dict(r) for r in c.execute("SELECT * FROM hunts ORDER BY id DESC LIMIT ?",(max(1,min(limit,100)),)).fetchall()]; c.close(); return {"runs":rows}

@app.get("/verdicts")
def verdicts():
    ps=products(200); return {v:sum(p.get("verdict")==v for p in ps) for v in ["EXCEPTIONAL","STRONG WINNER","TEST","WATCH"]}

@app.post("/auto-hunt/{category}")
async def auto_hunt(category:str, background_tasks:BackgroundTasks, dry_run:bool=False):
    if category not in CATEGORIES and category!="all": return {"status":"error","message":"Category must be kitchen_utility, home_decor or all"}
    # Keep HTTP responsive: execute in background.
    cats=list(CATEGORIES) if category=="all" else [category]
    for cat in cats:
        background_tasks.add_task(_bg_hunt,cat)
    return {"status":"success","message":"Hunt started","categories":cats}

async def _bg_hunt(cat):
    try: await execute_hunt(cat,HuntRequest(category=cat,min_score=60,min_price=40,max_price=150,max_cogs=35,min_markup=3,min_rating=4,min_reviews=50))
    except Exception as e: print(f"Hunt {cat} failed: {e}")

@app.post("/admin/hunt-now/{category}")
async def hunt_now(category:str):
    if category not in CATEGORIES: return {"status":"error","message":"Invalid category"}
    try: return await execute_hunt(category,HuntRequest(category=category,min_score=60,min_price=40,max_price=150,max_cogs=35,min_markup=3,min_rating=4,min_reviews=50))
    except Exception as e: return {"status":"error","message":str(e)}

@app.post("/hunt")
async def legacy_hunt(req:HuntRequest):
    cat=req.category if req.category in CATEGORIES else classify(req.keyword or "")
    if cat not in CATEGORIES: cat="kitchen_utility"
    try: return await execute_hunt(cat,req)
    except Exception as e: return {"status":"error","message":str(e)}

@app.get("/")
def root(): return {"service":"Product Hunter API","version":VERSION,"categories":list(CATEGORIES),"endpoints":["/health","/auto-hunt/kitchen_utility","/auto-hunt/home_decor","/auto-hunt/all","/products/accepted","/products/winners","/stats"]}
