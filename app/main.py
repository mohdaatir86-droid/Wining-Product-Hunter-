from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Any
from difflib import SequenceMatcher
import os, re, unicodedata, httpx, sqlite3, hashlib, json, asyncio, time
from datetime import datetime, timezone
from urllib.parse import quote_plus

VERSION="6.3.1"
DB_PATH=os.getenv("PRODUCT_HUNTER_DB","product_hunter.db")

app=FastAPI(title="Product Hunter API", version=VERSION)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])

CATEGORIES={
 "kitchen_utility":["kitchen","kitchen utility","organizer","storage","pantry","drawer","countertop","sink","spice","cooking","meal prep","dish","utensil","cabinet","shelf","rack","holder","chopping","cutting","cleaning","baking"],
 "home_decor":["home decor","home decoration","decor","decorative","wall","table decor","living room","bedroom","aesthetic","minimalist","home accessory","art","poster","frame","cushion","throw","shelf","vase","planter","ornament","sculpture","accent"]
}
WEIGHTS={"wow_factor":10,"price":8,"cogs":6,"markup":8,"perceived_value":8,"pain_point":8,"daily_use":7,"golden_customer":6,"lightweight":5,"safe_shipping":5,"evergreen":5,"quality":8,"proven_seller":8,"content_potential":8}

class HuntRequest(BaseModel):
    category: str = "all"
    keyword: Optional[str] = None
    marketplace: str = "US"
    country: Optional[str] = None
    supplier_ship_to: Optional[str] = None
    currency: Optional[str] = None
    max_products: int = 20
    amazon_pages: int = 2
    supplier_pages: int = 2
    max_cogs: float = 35
    min_markup: float = 3
    min_rating: float = 4
    min_reviews: int = 50
    min_score: float = 60


MARKETS = {
    "US": {"country": "US", "currency": "USD", "amazon_host": "www.amazon.com"},
    "IN": {"country": "IN", "currency": "INR", "amazon_host": "www.amazon.in"},
    "GB": {"country": "GB", "currency": "GBP", "amazon_host": "www.amazon.co.uk"},
    "CA": {"country": "CA", "currency": "CAD", "amazon_host": "www.amazon.ca"},
    "AU": {"country": "AU", "currency": "AUD", "amazon_host": "www.amazon.com.au"},
    "DE": {"country": "DE", "currency": "EUR", "amazon_host": "www.amazon.de"},
    "AE": {"country": "AE", "currency": "AED", "amazon_host": "www.amazon.ae"},
    "SA": {"country": "SA", "currency": "SAR", "amazon_host": "www.amazon.sa"},
}


def market_cfg(req: HuntRequest):
    code = (req.marketplace or req.country or "US").upper()
    cfg = MARKETS.get(code, MARKETS["US"]).copy()
    if req.country and req.country.upper() in MARKETS:
        cfg["country"] = req.country.upper()
    if req.supplier_ship_to:
        cfg["supplier_ship_to"] = req.supplier_ship_to.upper()
    else:
        cfg["supplier_ship_to"] = cfg["country"]
    if req.currency:
        cfg["currency"] = req.currency.upper()
    cfg["code"] = code
    return cfg


def db():
    c=sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory=sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE IF NOT EXISTS products (fingerprint TEXT PRIMARY KEY, data TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS hunts (id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT, status TEXT, started_at TEXT, finished_at TEXT, demand_items INTEGER DEFAULT 0, supplier_items INTEGER DEFAULT 0, accepted INTEGER DEFAULT 0, error TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS pipeline (run_id INTEGER PRIMARY KEY, category TEXT, stage TEXT, message TEXT, seen INTEGER DEFAULT 0, suppliers INTEGER DEFAULT 0, accepted INTEGER DEFAULT 0, updated_at TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS scraper_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, source TEXT, fetched_at TEXT, item_count INTEGER, sample_json TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS hunt_diagnostics (run_id INTEGER PRIMARY KEY, diagnostics_json TEXT, updated_at TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS sales_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, product_key TEXT, run_id INTEGER, source TEXT, observed_at TEXT, cumulative_sales INTEGER, cumulative_reviews INTEGER, payload_json TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS demand_signal_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, product_key TEXT, run_id INTEGER, observed_at TEXT, marketplace_rank INTEGER, search_interest REAL, pinterest_wow REAL, pinterest_mom REAL, pinterest_yoy REAL, sales_cumulative INTEGER, payload_json TEXT)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_demand_signal_product_time ON demand_signal_snapshots(product_key, observed_at)")
    c.execute("CREATE TABLE IF NOT EXISTS trend_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, query TEXT, country TEXT, source TEXT, observed_at TEXT, current_interest REAL, momentum_pct REAL, payload_json TEXT)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trend_query_time ON trend_snapshots(query, country, source, observed_at)")
    c.execute("CREATE TABLE IF NOT EXISTS source_health (source TEXT PRIMARY KEY, marketplace TEXT, country TEXT, status TEXT, success_count INTEGER DEFAULT 0, failure_count INTEGER DEFAULT 0, empty_count INTEGER DEFAULT 0, avg_ms REAL, last_success TEXT, last_error TEXT, last_records INTEGER DEFAULT 0, fields_json TEXT, updated_at TEXT)")
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
    v = first(x,["reviews","reviews_count","reviewCount","review_count","ratingsTotal","ratings_total"],0)
    return integer(v)

def sales_of(x):
    """Cumulative order/sold count only. Rolling-window values are intentionally excluded."""
    v = first(x,["orders_count","order_count","orderCount","sold_count","soldCount","units_sold","unitsSold","sales_count","salesCount","sales"],None)
    return integer(v, None) if v is not None else None

def rank_of(x):
    """Marketplace sales/bestseller rank only; SERP position is intentionally not treated as sales rank."""
    v = first(x,["best_seller_rank","bestSellerRank","sales_rank","salesRank","bsr","best_seller_rank_position","bestseller_rank"],None)
    if isinstance(v, dict):
        v = first(v,["rank","value","position"],None)
    r = integer(v, None) if v is not None else None
    return r if r is not None and r > 0 else None

def trend_query_of(name, category):
    stop={"the","and","with","for","from","set","pack","piece","pieces","bpa","free","new","best","home","decor","kitchen"}
    words=[w for w in re.findall(r"[A-Za-z]{3,}", name.lower()) if w not in stop]
    # Keep the phrase short enough for Trends while preserving product intent.
    q=" ".join(words[:5]).strip()
    return q or category.replace("_"," ")

def pct_change(new, old):
    if new is None or old in (None,0): return None
    return round((new-old)/abs(old)*100,1)

async def serpapi_trends(queries, country="US"):
    key=os.getenv("SERPAPI_API_KEY")
    if not key or not queries: return {}
    out={}
    # SerpApi allows up to 5 queries in one TIMESERIES request.
    for i in range(0,len(queries),5):
        batch=[q for q in queries[i:i+5] if q]
        if not batch: continue
        params={"engine":"google_trends","q":",".join(batch),"date":"today 12-m","data_type":"TIMESERIES","geo":country,"api_key":key}
        async with httpx.AsyncClient(timeout=45) as c:
            r=await c.get("https://serpapi.com/search.json",params=params)
            if r.status_code>=400:
                raise RuntimeError(f"SerpApi Google Trends HTTP {r.status_code}: {r.text[:500]}")
            data=r.json()
        timeline=data.get("interest_over_time",{}).get("timeline_data",[]) if isinstance(data,dict) else []
        byq={q:[] for q in batch}
        for point in timeline:
            for item in point.get("values",[]) if isinstance(point,dict) else []:
                q=item.get("query")
                val=item.get("extracted_value", item.get("value"))
                if q in byq and val is not None:
                    byq[q].append(num(val,None))
        for q,vals in byq.items():
            vals=[x for x in vals if x is not None]
            current=vals[-1] if vals else None
            prior=vals[-5] if len(vals)>=5 else (vals[-2] if len(vals)>=2 else None)
            momentum=pct_change(current,prior)
            out[q]={"source":"google_trends","current_interest":current,"momentum_pct":momentum,"points":len(vals)}
            c=db(); c.execute("INSERT INTO trend_snapshots(query,country,source,observed_at,current_interest,momentum_pct,payload_json) VALUES(?,?,?,?,?,?,?)",(q,country,"google_trends",now(),current,momentum,json.dumps({"values":vals[-12:]},ensure_ascii=False))); c.commit(); c.close()
    return out

async def pinterest_trends(country="US", limit=50):
    token=os.getenv("PINTEREST_ACCESS_TOKEN")
    if not token: return {}
    # Pinterest currently exposes today's top/growing keyword trends, not arbitrary historical queries.
    url=f"https://api.pinterest.com/v5/trends/keywords/{country}/top/growing"
    async with httpx.AsyncClient(timeout=45) as c:
        r=await c.get(url,params={"limit":min(limit,50)},headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
        if r.status_code>=400:
            raise RuntimeError(f"Pinterest Trends HTTP {r.status_code}: {r.text[:500]}")
        data=r.json()
    out={}
    for item in data.get("trends",[]) if isinstance(data,dict) else []:
        q=str(item.get("keyword","")).strip().lower()
        if q:
            out[q]={"wow":num(item.get("pct_growth_wow"),None),"mom":num(item.get("pct_growth_mom"),None),"yoy":num(item.get("pct_growth_yoy"),None),"time_series":item.get("time_series",{})}
    return out

def best_pinterest_match(name, ptrends):
    if not ptrends: return None
    words=set(re.findall(r"[a-z]{3,}", norm(name)))
    best=None; best_score=0
    for q,vals in ptrends.items():
        qw=set(re.findall(r"[a-z]{3,}", norm(q)))
        overlap=len(words & qw)/max(1,len(qw))
        if overlap>best_score:
            best_score=overlap; best=(q,vals)
    return best if best_score>=0.34 else None

def save_demand_signal(product_key,run_id,rank,google,pinterest,sales,payload=None):
    c=db(); c.execute("INSERT INTO demand_signal_snapshots(product_key,run_id,observed_at,marketplace_rank,search_interest,pinterest_wow,pinterest_mom,pinterest_yoy,sales_cumulative,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?)",(product_key,run_id,now(),rank,google.get("current_interest") if google else None,pinterest.get("wow") if pinterest else None,pinterest.get("mom") if pinterest else None,pinterest.get("yoy") if pinterest else None,sales,json.dumps(payload or {},ensure_ascii=False,default=str))); c.commit(); c.close()

def demand_signal_history(product_key,limit=50):
    c=db(); rows=c.execute("SELECT * FROM demand_signal_snapshots WHERE product_key=? ORDER BY id DESC LIMIT ?",(product_key,limit)).fetchall(); c.close(); return [dict(r) for r in rows]

def rank_velocity(history):
    pts=[]
    for r in sorted(history,key=lambda x:x.get("observed_at") or ""):
        rank=r.get("marketplace_rank")
        dt=_parse_dt(r.get("observed_at"))
        if rank and dt: pts.append((dt,int(rank)))
    out={"current_rank":pts[-1][1] if pts else None,"velocity_24h":None,"velocity_7d":None,"velocity_30d":None,"acceleration":None,"status":"collecting","snapshots":len(pts)}
    if not pts: return out
    latest_t,latest=pts[-1]
    def at_or_before(seconds):
        cutoff=latest_t.timestamp()-seconds
        prior=[p for p in pts if p[0].timestamp()<=cutoff]
        return prior[-1][1] if prior else None
    for seconds,key in [(86400,"velocity_24h"),(7*86400,"velocity_7d"),(30*86400,"velocity_30d")]:
        old=at_or_before(seconds)
        if old and latest:
            out[key]=round((old-latest)/old*100,1)
    if out["velocity_7d"] is not None:
        out["status"]="exploding" if out["velocity_7d"]>=50 else "rising" if out["velocity_7d"]>=15 else "growing" if out["velocity_7d"]>0 else "declining"
    elif len(pts)>=2: out["status"]="building"
    return out

def demand_momentum_score(rankv, google, pinterest, salesv, reviews):
    signals=[]
    if rankv.get("velocity_7d") is not None:
        signals.append(("rank",max(0,min(100,50+rankv["velocity_7d"])),35))
    if google and google.get("momentum_pct") is not None:
        signals.append(("google",max(0,min(100,50+google["momentum_pct"])),25))
    if pinterest and pinterest.get("mom") is not None:
        signals.append(("pinterest",max(0,min(100,50+pinterest["mom"])),15))
    if salesv is not None:
        signals.append(("sales",max(0,min(100,50+salesv*10)),15))
    # Reviews are a weak competition proxy: very high review counts imply a more mature niche.
    if reviews is not None:
        comp=max(0,min(100,100-(max(0,reviews-50)/950)*100))
        signals.append(("competition",comp,10))
    if not signals: return None
    total=sum(v*w for _,v,w in signals)/sum(w for _,_,w in signals)
    return round(total,1)


def desc_of(x): return str(first(x,["description","productDescription","product_description","features","bullets","category"],""))

def image_of(x):
    v = first(x,["image","imageUrl","image_url","thumbnail","thumbnailUrl","imageURL"],None)
    if isinstance(v, list):
        return v[0] if v else None
    if isinstance(v, dict):
        return first(v,["url","src","link"],None)
    return v

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
      "price": 10 if 50<=selling<=80 else 8 if selling>=40 else 6 if selling>0 else 0,
      "cogs": 10 if 10<=cogs<=35 else 7 if cogs>0 else 6,
      "markup": 10 if markup>=4 else 9 if markup>=3 else 6 if not cogs else 0,
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


def early_viral_score(p):
    rv=p.get("rank_velocity") or {}
    rank7=rv.get("velocity_7d")
    rank24=rv.get("velocity_24h")
    sales24=p.get("sales_last_24h")
    sales7=p.get("sales_last_7d")
    g=p.get("google_trends") or {}
    pv=p.get("pinterest_trends") or {}
    review=p.get("reviews") or 0
    # Momentum-first model; missing signals do not become fake zeros.
    vals=[]
    if rank7 is not None: vals.append(("rank7",max(0,min(100,50+rank7*1.1)),25))
    if rank24 is not None: vals.append(("rank24",max(0,min(100,50+rank24*1.2)),10))
    if sales7 is not None: vals.append(("sales7",min(100,50+min(50,sales7)*1.0),15))
    if sales24 is not None: vals.append(("sales24",min(100,50+min(50,sales24)*1.5),10))
    if g.get("momentum_pct") is not None: vals.append(("google",max(0,min(100,50+g["momentum_pct"])),15))
    if pv.get("mom") is not None: vals.append(("pinterest",max(0,min(100,50+pv["mom"])),10))
    # Lower review counts can indicate earlier stage, but only as a weak maturity signal.
    maturity=max(0,min(100,100-(max(0,review-100)/4900)*100))
    vals.append(("maturity",maturity,5))
    content=p.get("breakdown",{}).get("content_potential",6)*10
    vals.append(("content",content,10))
    if not vals: return None,"INSUFFICIENT DATA"
    score=round(sum(v*w for _,v,w in vals)/sum(w for _,_,w in vals),1)
    if score>=85: label="EARLY VIRAL"
    elif score>=72: label="RISING FAST"
    elif score>=58: label="RISING"
    else: label="STABLE"
    return score,label

def opportunity_score(p):
    demand=p.get("demand_momentum_score")
    viral=p.get("early_viral_score")
    quality=float(p.get("score") or 0)
    comp=max(0,min(100,100-(float(p.get("reviews") or 0)/5000)*100))
    parts=[]
    for val,w in [(demand,20),(viral,20),(p.get("rank_velocity",{}).get("velocity_7d"),15),(viral,15),(quality,10),(100 if p.get("markup") and p.get("markup")>=3 else 60,8),(comp,5),(float(p.get("breakdown",{}).get("content_potential",6))*10,5),(float(p.get("breakdown",{}).get("wow_factor",6))*10,2)]:
        if val is not None:
            if val in (p.get("rank_velocity",{}).get("velocity_7d"),): val=max(0,min(100,50+float(val)))
            parts.append((float(val),w))
    return round(sum(v*w for v,w in parts)/sum(w for _,w in parts),1) if parts else quality

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


async def run_crawlbase(target_url, scraper, country: Optional[str] = None):
    """Call Crawlbase Crawling API and return the scraper's structured JSON."""
    token = os.getenv("CRAWLBASE_TOKEN")
    if not token:
        raise RuntimeError("CRAWLBASE_TOKEN is not configured")
    params = {"token": token, "url": target_url, "scraper": scraper}
    if country:
        params["country"] = country.upper()
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

async def run_crawlbase_amazon(queries, pages=1, req: Optional[HuntRequest] = None):
    items = []
    cfg = market_cfg(req or HuntRequest())
    for q in queries[:5]:
        for page in range(1, max(1, min(pages, 3)) + 1):
            url = f"https://{cfg['amazon_host']}/s?k={quote_plus(q)}&page={page}"
            data = await run_crawlbase(url, "amazon-serp", cfg["country"])
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

async def run_crawlbase_aliexpress(queries, pages=1, req: Optional[HuntRequest] = None):
    items = []
    cfg = market_cfg(req or HuntRequest())
    for q in queries[:5]:
        # AliExpress SERP accepts search/wholesale URLs. Geo-route the request to the selected market.
        url = f"https://www.aliexpress.com/wholesale?SearchText={quote_plus(q)}"
        data = await run_crawlbase(url, "aliexpress-serp", cfg["country"])
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


def _scrapingant_rows(payload):
    """Normalize ScrapingAnt /extract responses across common response shapes."""
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("products","items","results","data"):
        val=payload.get(key)
        if isinstance(val,list):
            return [x for x in val if isinstance(x,dict)]
        if isinstance(val,dict):
            for k in ("products","items","results"):
                if isinstance(val.get(k),list):
                    return [x for x in val[k] if isinstance(x,dict)]
    return [payload] if name_of(payload) else []

async def run_scrapingant_extract(target_url, properties, country="US"):
    key=os.getenv("SCRAPINGANT_API_KEY")
    if not key:
        raise RuntimeError("SCRAPINGANT_API_KEY is not configured")
    params={
        "url":target_url,
        "x-api-key":key,
        "extract_properties":json.dumps(properties),
        "browser":"true",
        "proxy_country":country.upper(),
        "timeout":25,
    }
    t0=time.perf_counter()
    async with httpx.AsyncClient(timeout=35) as c:
        r=await c.get("https://api.scrapingant.com/v2/extract",params=params)
    ms=round((time.perf_counter()-t0)*1000,1)
    if r.status_code>=400:
        raise RuntimeError(f"ScrapingAnt HTTP {r.status_code}: {r.text[:700]}")
    try: payload=r.json()
    except Exception as e: raise RuntimeError(f"ScrapingAnt invalid JSON: {e}")
    rows=_scrapingant_rows(payload)
    record_source_health("scrapingant","amazon" if "amazon." in target_url else "aliexpress",country,"success",len(rows),ms,None,properties)
    return rows

async def run_scrapingant_amazon(queries, req=None):
    cfg=market_cfg(req or HuntRequest())
    props=[
        "product title",
        "price(number)",
        "rating(number)",
        "review count(number)",
        "product URL",
        "image URL",
        "ASIN",
        "best seller rank",
        "bought last month",
    ]
    out=[]
    for q in queries[:3]:
        url=f"https://{cfg['amazon_host']}/s?k={quote_plus(q)}"
        try:
            rows=await run_scrapingant_extract(url,props,cfg["country"])
        except Exception as e:
            record_source_health("scrapingant","amazon",cfg["country"],"failure",0,0,str(e),props)
            raise
        for x in rows:
            y=dict(x)
            if name_of(y): out.append(y)
    return out

async def run_scrapingant_aliexpress(queries, req=None):
    cfg=market_cfg(req or HuntRequest())
    props=[
        "product title",
        "price(number)",
        "rating(number)",
        "review count(number)",
        "product URL",
        "image URL",
        "orders count(number)",
        "product ID",
    ]
    out=[]
    for q in queries[:3]:
        url=f"https://www.aliexpress.com/wholesale?SearchText={quote_plus(q)}"
        try:
            rows=await run_scrapingant_extract(url,props,cfg["country"])
        except Exception as e:
            record_source_health("scrapingant","aliexpress",cfg["country"],"failure",0,0,str(e),props)
            raise
        out.extend(rows)
    return out


async def test_zenrows():
    """Live credential test. This makes one simple ZenRows request and may consume 1 credit."""
    key=os.getenv("ZENROWS_API_KEY")
    if not key:
        return {"provider":"zenrows","configured":False,"status":"NOT_CONFIGURED"}
    t0=time.perf_counter()
    try:
        params={"url":"https://example.com","apikey":key}
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.get("https://api.zenrows.com/v1/",params=params)
        ms=round((time.perf_counter()-t0)*1000,1)
        ok=200 <= r.status_code < 400
        return {"provider":"zenrows","configured":True,"status":"CONNECTED" if ok else "FAILED","http_status":r.status_code,"latency_ms":ms,
                "note":"Live test request; may consume ZenRows credits." if ok else r.text[:300]}
    except Exception as e:
        return {"provider":"zenrows","configured":True,"status":"FAILED","error":str(e)[:500]}

async def test_dataforseo():
    """Credential/account test using DataForSEO's zero-cost user_data endpoint."""
    login=os.getenv("DATAFORSEO_LOGIN")
    password=os.getenv("DATAFORSEO_PASSWORD")
    if not login or not password:
        return {"provider":"dataforseo","configured":False,"status":"NOT_CONFIGURED"}
    t0=time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.get("https://api.dataforseo.com/v3/appendix/user_data",
                          auth=(login,password),
                          headers={"Content-Type":"application/json"})
        ms=round((time.perf_counter()-t0)*1000,1)
        if r.status_code>=400:
            return {"provider":"dataforseo","configured":True,"status":"FAILED","http_status":r.status_code,"latency_ms":ms,"error":r.text[:500]}
        data=r.json()
        api_ok=data.get("status_code")==20000
        result=(data.get("tasks") or [{}])[0].get("result") or [{}]
        account=result[0] if isinstance(result,list) and result else {}
        money=account.get("money") if isinstance(account,dict) else {}
        return {"provider":"dataforseo","configured":True,"status":"CONNECTED" if api_ok else "FAILED",
                "http_status":r.status_code,"api_status_code":data.get("status_code"),
                "api_message":data.get("status_message"),"latency_ms":ms,
                "login_hint":str(account.get("login",""))[:3]+"***" if account.get("login") else None,
                "balance_usd":money.get("balance") if isinstance(money,dict) else None,
                "test_cost_usd":0}
    except Exception as e:
        return {"provider":"dataforseo","configured":True,"status":"FAILED","error":str(e)[:500]}

def record_source_health(source,marketplace,country,status,records,elapsed_ms,error=None,fields=None):
    c=db()
    row=c.execute("SELECT success_count,failure_count,empty_count,avg_ms,last_success FROM source_health WHERE source=?",(f"{source}:{marketplace}",)).fetchone()
    sc,fc,ec,avg=(row if row else (0,0,0,None))
    if status=="success":
        sc+=1
        if records==0: ec+=1
        avg=round((float(avg or 0)*(sc-1)+float(elapsed_ms or 0))/sc,1)
    else: fc+=1
    state="HEALTHY" if sc>=2 and fc<=max(2,sc) else ("DEGRADED" if sc else "FAILED")
    c.execute("""INSERT INTO source_health(source,marketplace,country,status,success_count,failure_count,empty_count,avg_ms,last_success,last_error,last_records,fields_json,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(source) DO UPDATE SET marketplace=excluded.marketplace,country=excluded.country,status=excluded.status,
    success_count=excluded.success_count,failure_count=excluded.failure_count,empty_count=excluded.empty_count,avg_ms=excluded.avg_ms,
    last_success=excluded.last_success,last_error=excluded.last_error,last_records=excluded.last_records,fields_json=excluded.fields_json,updated_at=excluded.updated_at""",
    (f"{source}:{marketplace}",marketplace,country,state,sc,fc,ec,avg,now() if status=="success" else (row[4] if row else None),error,records,json.dumps(fields or []),now()))
    c.commit(); c.close()

async def serpapi_amazon_product(asin,cfg):
    key=os.getenv("SERPAPI_API_KEY")
    if not key or not asin: return {}
    params={"engine":"amazon_product","asin":asin,"amazon_domain":cfg["amazon_host"].replace("www.",""),"shipping_location":cfg["country"],"api_key":key}
    async with httpx.AsyncClient(timeout=35) as c:
        r=await c.get("https://serpapi.com/search.json",params=params)
    if r.status_code>=400:
        raise RuntimeError(f"SerpApi Amazon Product HTTP {r.status_code}: {r.text[:500]}")
    return r.json()

def bsr_from_serpapi(data):
    pd=data.get("product_details",{}) if isinstance(data,dict) else {}
    ranks=pd.get("best_sellers_rank",[])
    if not isinstance(ranks,list): return None
    vals=[integer(x.get("extracted_rank"),None) for x in ranks if isinstance(x,dict)]
    vals=[v for v in vals if v and v>0]
    return min(vals) if vals else None

def set_pipeline(run_id, category, stage, message, seen=0, suppliers=0, accepted=0):
    c=db(); c.execute("INSERT INTO pipeline(run_id,category,stage,message,seen,suppliers,accepted,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET stage=excluded.stage,message=excluded.message,seen=excluded.seen,suppliers=excluded.suppliers,accepted=excluded.accepted,updated_at=excluded.updated_at",(run_id,category,stage,message,seen,suppliers,accepted,now())); c.commit(); c.close()

def save_scraper_snapshot(run_id, source, items):
    sample = items[:25] if isinstance(items,list) else []
    c=db(); c.execute("INSERT INTO scraper_snapshots(run_id,source,fetched_at,item_count,sample_json) VALUES(?,?,?,?,?)",(run_id,source,now(),len(items) if isinstance(items,list) else 0,json.dumps(sample,ensure_ascii=False,default=str))); c.commit(); c.close()

def save_diagnostics(run_id, diagnostics):
    c=db(); c.execute("INSERT INTO hunt_diagnostics(run_id,diagnostics_json,updated_at) VALUES(?,?,?) ON CONFLICT(run_id) DO UPDATE SET diagnostics_json=excluded.diagnostics_json,updated_at=excluded.updated_at",(run_id,json.dumps(diagnostics,ensure_ascii=False),now())); c.commit(); c.close()


def save_sales_snapshot(product_key, run_id, source, cumulative_sales, cumulative_reviews, payload):
    if cumulative_sales is None and cumulative_reviews is None:
        return
    c=db(); c.execute(
        "INSERT INTO sales_snapshots(product_key,run_id,source,observed_at,cumulative_sales,cumulative_reviews,payload_json) VALUES(?,?,?,?,?,?,?)",
        (product_key,run_id,source,now(),cumulative_sales,cumulative_reviews,json.dumps(payload or {},ensure_ascii=False,default=str))
    ); c.commit(); c.close()

def sales_history(product_key, source=None, limit=50):
    c=db()
    if source:
        rows=c.execute("SELECT observed_at,cumulative_sales,cumulative_reviews,run_id,source FROM sales_snapshots WHERE product_key=? AND source=? ORDER BY id DESC LIMIT ?",(product_key,source,limit)).fetchall()
    else:
        rows=c.execute("SELECT observed_at,cumulative_sales,cumulative_reviews,run_id,source FROM sales_snapshots WHERE product_key=? ORDER BY id DESC LIMIT ?",(product_key,limit)).fetchall()
    c.close(); return [dict(r) for r in rows]

def _parse_dt(s):
    try: return datetime.fromisoformat(str(s).replace("Z","+00:00"))
    except: return None

def velocity_metrics(history):
    now_dt=datetime.now(timezone.utc)
    points=[]
    for r in sorted(history,key=lambda x:x.get("observed_at") or ""):
        dt=_parse_dt(r.get("observed_at")); val=r.get("cumulative_sales")
        if dt and val is not None: points.append((dt,int(val)))
    latest = points[-1][1] if points else None
    out={"latest_cumulative_sales":latest,"last_24h":None,"last_7d":None,"data_status":"collecting","snapshots":len(points),"first_observed_at":points[0][0].isoformat() if points else None,"last_observed_at":points[-1][0].isoformat() if points else None}
    if not points: return out
    for horizon, key in [(24*3600,"last_24h"),(7*24*3600,"last_7d")]:
        cutoff=now_dt.timestamp()-horizon
        prior=[p for p in points if p[0].timestamp()<=cutoff]
        if prior:
            out[key]=max(0,latest-prior[-1][1])
    if out["last_24h"] is not None or out["last_7d"] is not None: out["data_status"]="measured"
    elif len(points)>=2: out["data_status"]="building"
    return out

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
    cfg = market_cfg(req)
    set_pipeline(run_id, category, "starting", f"Preparing product hunt for {cfg['code']} ({cfg['currency']})")

    demand = []
    supplier = []
    source_errors = []

    try:
        # Multi-source collection: ScrapingAnt + Crawlbase + Apify.
        # Providers are complementary; failures never fabricate or block other sources.
        demand_sources=[]; supplier_sources=[]
        if os.getenv("SCRAPINGANT_API_KEY"):
            try:
                set_pipeline(run_id, category, "scraping_scrapingant", "Fetching ScrapingAnt demand + supplier data")
                sa_d=await run_scrapingant_amazon(queries,req)
                sa_s=await run_scrapingant_aliexpress(queries,req)
                demand_sources.append(("scrapingant",sa_d)); supplier_sources.append(("scrapingant",sa_s))
                save_scraper_snapshot(run_id,"scrapingant_amazon",sa_d)
                save_scraper_snapshot(run_id,"scrapingant_aliexpress",sa_s)
            except Exception as e:
                source_errors.append(f"scrapingant: {e}")

        if os.getenv("CRAWLBASE_TOKEN"):
            try:
                set_pipeline(run_id, category, "scraping_crawlbase", "Fetching Crawlbase demand + supplier data")
                cb_d=await run_crawlbase_amazon(queries,req.amazon_pages,req)
                cb_s=await run_crawlbase_aliexpress(queries,req.supplier_pages,req)
                demand_sources.append(("crawlbase",cb_d)); supplier_sources.append(("crawlbase",cb_s))
                save_scraper_snapshot(run_id,"crawlbase_amazon",cb_d)
                save_scraper_snapshot(run_id,"crawlbase_aliexpress",cb_s)
            except Exception as e:
                source_errors.append(f"crawlbase: {e}")

        # Apify remains the third fallback/provider where configured.
        if os.getenv("APIFY_API_TOKEN"):
            token=os.getenv("APIFY_API_TOKEN")
            da=os.getenv("APIFY_DEMAND_ACTOR_ID"); sa=os.getenv("APIFY_SUPPLIER_ACTOR_ID")
            if da:
                try:
                    demand_input={"searchQueries":queries,"marketplace":cfg["code"],"maxProductsPerSearch":max(5,min(req.max_products,50)),"maxSearchPages":max(1,min(req.amazon_pages,5)),"proxyConfiguration":{"useApifyProxy":True,"apifyProxyGroups":["RESIDENTIAL"]}}
                    ap_d=await run_actor(da,token,demand_input)
                    demand_sources.append(("apify",ap_d)); save_scraper_snapshot(run_id,"apify_amazon",ap_d)
                except Exception as e: source_errors.append(f"apify_amazon: {e}")
            if sa:
                try:
                    supplier_input={"mode":"search","searchQuery":" ".join(queries[:2]),"region":cfg["code"].lower() if cfg["code"] in {"US","GB","CA","AU","DE"} else "com","currency":cfg["currency"],"language":"en_US","sortBy":"orders_desc","priceMin":0,"priceMax":max(1,int(req.max_cogs)),"shipTo":cfg["supplier_ship_to"],"maxItems":max(20,min(req.max_products*5,100)),"maxPages":max(1,min(req.supplier_pages,10))}
                    ap_s=await run_actor(sa,token,supplier_input)
                    supplier_sources.append(("apify",ap_s)); save_scraper_snapshot(run_id,"apify_aliexpress",ap_s)
                except Exception as e: source_errors.append(f"apify_aliexpress: {e}")

        def merge_sources(groups):
            merged={}
            for source,rows in groups:
                for row in rows if isinstance(rows,list) else []:
                    n=name_of(row); u=first(row,["url","productUrl","product_url","detailPageURL","link"],None)
                    key=norm(first(row,["asin","productId","product_id","sku"],"")) or fp(n,u)
                    if not n: continue
                    if key not in merged:
                        merged[key]={"row":dict(row),"sources":[source]}
                    else:
                        merged[key]["sources"].append(source)
                        # Prefer the row with richer fields.
                        if len(json.dumps(row,default=str))>len(json.dumps(merged[key]["row"],default=str)):
                            merged[key]["row"]=dict(row)
            return [(v["row"],v["sources"]) for v in merged.values()]
        demand_merged=merge_sources(demand_sources)
        supplier_merged=merge_sources(supplier_sources)
        demand=[r for r,_ in demand_merged]
        supplier=[r for r,_ in supplier_merged]
        set_pipeline(run_id,category,"sources_merged",f"Merged {len(demand)} unique demand and {len(supplier)} supplier records",seen=len(demand),suppliers=len(supplier))

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
            "rejected_rating_reviews": 0,
            "rejected_supplier_match": 0,
            "rejected_cogs_markup": 0,
            "rejected_score": 0,
        }

        set_pipeline(run_id, category, "filtering", "Applying category, quality, COGS/markup and shipping-safety rules — price is NOT filtered", seen=len(demand), suppliers=len(supplier))
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

            # Rating/review thresholds are scoring signals, not visibility gates.
            # This prevents promising early products from disappearing before momentum is measured.

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
            if best:
                supplier_name, cogs, sx = best
            else:
                supplier_name, cogs, sx = None, 0, {}
            # Supplier economics are scored when available, but never hide a demand candidate.

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
                "product_key": fp(n, first(x, ["url", "productUrl", "product_url", "detailPageURL"], None)),
                "name": n,
                "category_key": category,
                "category": "Kitchen Utility" if category == "kitchen_utility" else "Home Décor",
                "selling_price": round(sell, 2),
                "cost_of_goods": round(cogs, 2) if cogs > 0 else None,
                "markup": round(sell / cogs, 2) if cogs > 0 else None,
                "rating": rating,
                "reviews": reviews,
                "country": cfg["country"],
                "marketplace": cfg["code"],
                "currency": cfg["currency"],
                "sales_last_24h": None,
                "sales_last_7d": None,
                "sales_data_status": "collecting",
                "score": score,
                "winner_score": score,
                "verdict": verdict(score),
                "supplier_name": supplier_name,
                "supplier_match_score": round(bestsim, 3) if best else 0,
                "image": image_of(x),
                "url": first(x, ["url", "productUrl", "product_url", "detailPageURL"], None),
                "supplier_url": first(sx, ["url", "productUrl", "product_url", "detailUrl"], None),
                "breakdown": breakdown,
                "discovered_at": now(),
                "source": "multi_source",
                "source_sales_cumulative": sales_of(x),
                "marketplace_rank": rank_of(x),
                "trend_query": trend_query_of(n, category),
                "supplier_orders_cumulative": sales_of(sx),
            }
            save_product(p)
            save_sales_snapshot(p["product_key"], run_id, "demand", sales_of(x), reviews, x)
            save_sales_snapshot(p["product_key"], run_id, "supplier", sales_of(sx), None, sx)
            save_demand_signal(p["product_key"], run_id, rank_of(x), {}, {}, sales_of(x), {"name":n,"source_payload":x})
            accepted.append(p)
            if len(accepted) >= max(1, min(req.max_products, 100)):
                break

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

        # Demand signals: Google Trends + Pinterest Trends are optional providers.
        # They never block the core hunt if credentials/access are missing.
        trend_queries=list(dict.fromkeys([p.get("trend_query") for p in accepted if p.get("trend_query")]))
        google_map={}; pinterest_map={}
        trend_errors=[]
        if os.getenv("SERPAPI_API_KEY") and trend_queries:
            try:
                google_map=await serpapi_trends(trend_queries[:20], cfg["country"])
            except Exception as e:
                trend_errors.append(f"google_trends: {e}")
        if os.getenv("PINTEREST_ACCESS_TOKEN"):
            try:
                pinterest_map=await pinterest_trends(cfg["country"],50)
            except Exception as e:
                trend_errors.append(f"pinterest_trends: {e}")

        enriched=[]
        for p in accepted:
            dh=sales_history(p["product_key"],"demand",100)
            sh=sales_history(p["product_key"],"supplier",100)
            dm=velocity_metrics(dh); sm=velocity_metrics(sh)
            p["sales_last_24h"] = dm["last_24h"]
            p["sales_last_7d"] = dm["last_7d"]
            p["sales_data_status"] = dm["data_status"]
            p["supplier_orders_last_24h"] = sm["last_24h"]
            p["supplier_orders_last_7d"] = sm["last_7d"]
            p["supplier_orders_data_status"] = sm["data_status"]
            rh=demand_signal_history(p["product_key"],100)
            rv=rank_velocity(rh)
            g=google_map.get(p.get("trend_query"),{})
            pm=best_pinterest_match(p.get("name",""),pinterest_map)
            pv=pm[1] if pm else {}
            p["rank_velocity"]=rv
            p["google_trends"]=g or {"status":"not_configured" if not os.getenv("SERPAPI_API_KEY") else "no_data"}
            p["pinterest_trends"]=(dict(pv,keyword=pm[0]) if pm else {"status":"not_configured" if not os.getenv("PINTEREST_ACCESS_TOKEN") else "no_match"})
            p["demand_momentum_score"]=demand_momentum_score(rv,g,pv,dm.get("last_7d"),p.get("reviews"))
            p["demand_momentum_verdict"]=("EXPLODING" if (p["demand_momentum_score"] or 0)>=85 else "RISING" if (p["demand_momentum_score"] or 0)>=70 else "GROWING" if (p["demand_momentum_score"] or 0)>=55 else "STABLE" if p["demand_momentum_score"] is not None else "COLLECTING")
            ev,label=early_viral_score(p)
            p["early_viral_score"]=ev
            p["viral_classification"]=label
            p["competition_score"]=round(max(0,min(100,100-(float(p.get("reviews") or 0)/5000)*100)),1)
            p["opportunity_score"]=opportunity_score(p)
            p["ai_verdict"]=label if label!="INSUFFICIENT DATA" else "INSUFFICIENT DATA"
            save_demand_signal(p["product_key"], run_id, rv.get("current_rank"), g, pv, p.get("source_sales_cumulative"), {"trend_query":p.get("trend_query"),"pinterest_keyword":pm[0] if pm else None})
            save_product(p)
            enriched.append(p)
        if trend_errors:
            source_errors.extend(trend_errors)

        # Dedicated Amazon product-ranking enrichment. SerpApi exposes actual Best Sellers Rank
        # on product pages; search-result position is never mislabeled as BSR.
        if os.getenv("SERPAPI_API_KEY"):
            for p in enriched[:min(len(enriched),20)]:
                asin=p.get("id") if str(p.get("id","")).startswith("B") else None
                if asin:
                    try:
                        rd=await serpapi_amazon_product(asin,cfg)
                        bsr=bsr_from_serpapi(rd)
                        if bsr:
                            p["marketplace_rank"]=bsr
                            p["rank_source"]="serpapi_amazon_product"
                            save_demand_signal(p["product_key"],run_id,bsr,p.get("google_trends") if isinstance(p.get("google_trends"),dict) else {},p.get("pinterest_trends") if isinstance(p.get("pinterest_trends"),dict) else {},p.get("source_sales_cumulative"),{"rank_source":"serpapi_amazon_product","bsr":bsr})
                            rh=demand_signal_history(p["product_key"],100)
                            p["rank_velocity"]=rank_velocity(rh)
                            ev,label=early_viral_score(p); p["early_viral_score"]=ev; p["viral_classification"]=label
                            p["opportunity_score"]=opportunity_score(p)
                            save_product(p)
                    except Exception as e:
                        source_errors.append(f"serpapi_rank:{p.get('id')}: {e}")

        return {
            "status": "success",
            "category": category,
            "run_id": run_id,
            "marketplace": cfg["code"],
            "country": cfg["country"],
            "currency": cfg["currency"],
            "demand_items": len(demand),
            "supplier_items": len(supplier),
            "accepted": len(enriched),
            "rejected": rejected,
            "source_errors": source_errors,
            "diagnostics": diagnostics,
            "products": sorted(enriched, key=lambda p: (p.get("opportunity_score") if p.get("opportunity_score") is not None else p.get("score",0)), reverse=True),
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
    return {"status":"ok","version":VERSION,"database":"connected","scrapingant_configured":bool(os.getenv("SCRAPINGANT_API_KEY")),"crawlbase_configured":bool(os.getenv("CRAWLBASE_TOKEN")),"apify_configured":bool(os.getenv("APIFY_API_TOKEN")),"zenrows_configured":bool(os.getenv("ZENROWS_API_KEY")),"dataforseo_configured":bool(os.getenv("DATAFORSEO_LOGIN") and os.getenv("DATAFORSEO_PASSWORD")),"categories_available":list(CATEGORIES.keys()),"markets":list(MARKETS.keys()),"price_filter":"disabled","sales_tracking":"snapshot-based","demand_signals":"rank_velocity+google_trends+pinterest_trends+early_viral","ranking_source":"serpapi_amazon_product","transparency_endpoints":["/pipeline/latest","/scraper-data/latest","/demand-signals/status","/source-health","/provider-tests"]}

@app.get("/sources/status")
def sources():
    return {
        "scrapingant":bool(os.getenv("SCRAPINGANT_API_KEY")),"crawlbase":bool(os.getenv("CRAWLBASE_TOKEN")),
        "apify":bool(os.getenv("APIFY_API_TOKEN")),
        "demand_actor":bool(os.getenv("APIFY_DEMAND_ACTOR_ID")),
        "supplier_actor":bool(os.getenv("APIFY_SUPPLIER_ACTOR_ID")),
        "serpapi_google_trends":bool(os.getenv("SERPAPI_API_KEY")),
        "pinterest_trends":bool(os.getenv("PINTEREST_ACCESS_TOKEN"))
    }

@app.get("/provider-tests")
async def provider_tests():
    """Live credential checks. DataForSEO is zero-cost; ZenRows test may consume one credit."""
    zen, dfs = await asyncio.gather(test_zenrows(), test_dataforseo())
    return {"status":"success","providers":{"zenrows":zen,"dataforseo":dfs}}

@app.get("/source-health")
def source_health():
    c=db(); rows=[dict(r) for r in c.execute("SELECT * FROM source_health ORDER BY updated_at DESC").fetchall()]; c.close()
    return {"status":"success","sources":rows}

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
        "note":"v6.0: price is not a hard filter; COGS/markup and quality rules remain. Sales velocity is derived only from repeated cumulative snapshots when the source provides cumulative sales/orders."
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
def accepted(limit:int=50,category:Optional[str]=None,marketplace:Optional[str]=None,country:Optional[str]=None):
    ps=products(limit,category)
    if marketplace: ps=[p for p in ps if str(p.get("marketplace","")).upper()==marketplace.upper()]
    if country: ps=[p for p in ps if str(p.get("country","")).upper()==country.upper()]
    return {"status":"success","count":len(ps),"products":ps}

@app.get("/products/detail/{product_key}")
def product_detail(product_key:str):
    c=db(); row=c.execute("SELECT data,created_at,updated_at FROM products WHERE fingerprint=?",(product_key,)).fetchone(); c.close()
    if not row: return {"status":"error","message":"Product not found"}
    p=json.loads(row[0]); dh=sales_history(product_key,"demand",100); sh=sales_history(product_key,"supplier",100)
    dm=velocity_metrics(dh); sm=velocity_metrics(sh)
    ds=demand_signal_history(product_key,100); rv=rank_velocity(ds)
    return {"status":"success","product":p,"created_at":row[1],"updated_at":row[2],"sales":{"demand":dm,"supplier_orders":sm},"demand_signals":{"rank_velocity":rv,"snapshots":ds},"demand_snapshots":dh,"supplier_snapshots":sh}

@app.get("/products/trending")
def trending(limit:int=50):
    ps=products(200)
    ps=[p for p in ps if p.get("demand_momentum_score") is not None]
    return {"status":"success","products":sorted(ps,key=lambda p:p.get("demand_momentum_score",0),reverse=True)[:max(1,min(limit,200))]}

@app.get("/demand-signals/status")
def demand_signal_status():
    return {
        "status":"success",
        "providers":{
            "rank_velocity":"active",
            "google_trends":"configured" if os.getenv("SERPAPI_API_KEY") else "not_configured",
            "pinterest_trends":"configured" if os.getenv("PINTEREST_ACCESS_TOKEN") else "not_configured"
        },
        "weights":{"rank_velocity":35,"google_trends":25,"pinterest_trends":15,"sales_signal":15,"competition_proxy":10}
    }

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
async def auto_hunt(category:str, background_tasks:BackgroundTasks, req: HuntRequest = HuntRequest(), dry_run:bool=False):
    if category not in CATEGORIES and category!="all": return {"status":"error","message":"Category must be kitchen_utility, home_decor or all"}
    cats=list(CATEGORIES) if category=="all" else [category]
    for cat in cats:
        body=req.model_copy(deep=True)
        body.category=cat
        background_tasks.add_task(_bg_hunt,cat,body)
    return {"status":"success","message":"Hunt started","categories":cats,"marketplace":market_cfg(req)["code"],"country":market_cfg(req)["country"]}

async def _bg_hunt(cat, req=None):
    try: await execute_hunt(cat, req or HuntRequest(category=cat,min_score=60,max_cogs=35,min_markup=3,min_rating=4,min_reviews=50))
    except Exception as e: print(f"Hunt {cat} failed: {e}")

@app.post("/admin/hunt-now/{category}")
async def hunt_now(category:str):
    if category not in CATEGORIES: return {"status":"error","message":"Invalid category"}
    try: return await execute_hunt(category,HuntRequest(category=category,min_score=60,max_cogs=35,min_markup=3,min_rating=4,min_reviews=50))
    except Exception as e: return {"status":"error","message":str(e)}

@app.post("/hunt")
async def legacy_hunt(req:HuntRequest):
    cat=req.category if req.category in CATEGORIES else classify(req.keyword or "")
    if cat not in CATEGORIES: cat="kitchen_utility"
    try: return await execute_hunt(cat,req)
    except Exception as e: return {"status":"error","message":str(e)}

@app.get("/")
def root(): return {"service":"Product Hunter API","version":VERSION,"categories":list(CATEGORIES),"markets":list(MARKETS),"price_filter":"disabled","sales_tracking":"snapshot-based","endpoints":["/health","/auto-hunt/kitchen_utility","/auto-hunt/home_decor","/auto-hunt/all","/products/accepted","/products/detail/{product_key}","/products/winners","/products/trending","/demand-signals/status","/stats","/pipeline/latest","/scraper-data/latest"]}
