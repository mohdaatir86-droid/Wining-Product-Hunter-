from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Any
from difflib import SequenceMatcher
import os, re, unicodedata, httpx, sqlite3, hashlib, json
from datetime import datetime, timezone

VERSION = "5.2.0"
DB_PATH = os.getenv("PRODUCT_HUNTER_DB", "product_hunter.db")

app = FastAPI(title="Product Hunter API", version=VERSION)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=False,
    allow_methods=["*"], allow_headers=["*"]
)

CATEGORIES = {
    "kitchen_utility": [
        "kitchen utility","kitchen organizer","kitchen storage","pantry organizer",
        "drawer organizer","countertop organizer","sink organizer","kitchen cleaning",
        "cooking tool","meal prep","spice organizer"
    ],
    "home_decor": [
        "home decor","home decoration","wall decor","table decor","living room decor",
        "bedroom decor","decorative storage","aesthetic home","minimalist decor",
        "home accessory"
    ],
}
WEIGHTS = {
    "wow_factor":10,"price":8,"cogs":6,"markup":8,"perceived_value":8,
    "pain_point":8,"daily_use":7,"golden_customer":6,"lightweight":5,
    "safe_shipping":5,"evergreen":5,"quality":8,"proven_seller":8,
    "content_potential":8
}

class HuntRequest(BaseModel):
    category: str = "all"
    keyword: Optional[str] = None
    marketplace: str = "US"
    supplier_ship_to: str = "US"
    currency: str = "USD"
    max_products: int = 20
    amazon_pages: int = 2
    supplier_pages: int = 2
    min_price: float = 40
    max_price: float = 150
    max_cogs: float = 35
    min_markup: float = 3
    min_rating: float = 4
    min_reviews: int = 50
    min_score: float = 60

def now():
    return datetime.now(timezone.utc).isoformat()

def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+"," ",s).strip()

def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("""CREATE TABLE IF NOT EXISTS products(
        fingerprint TEXT PRIMARY KEY, data TEXT NOT NULL,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS hunts(
        id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT, status TEXT,
        started_at TEXT, finished_at TEXT, demand_items INTEGER DEFAULT 0,
        supplier_items INTEGER DEFAULT 0, accepted INTEGER DEFAULT 0, error TEXT)""")
    c.commit()
    return c

def first(d, names, default=None):
    if not isinstance(d, dict):
        return default
    low = {str(k).lower(): v for k,v in d.items()}
    for n in names:
        v = low.get(n.lower())
        if v not in (None, ""):
            return v
    return default

def num(v, default=0):
    if isinstance(v,(int,float)):
        return float(v)
    if v is None:
        return default
    m = re.search(r"-?\d+(?:\.\d+)?", str(v).replace(",",""))
    return float(m.group()) if m else default

def integer(v, default=0):
    try: return int(num(v,default))
    except: return default

def name_of(x):
    return str(first(x,["name","title","productName","product_name","productTitle","product_title","itemTitle"],"")).strip()

def price_of(x):
    # Handles normal numeric fields and nested RapidAPI price objects.
    v = first(x,["price","currentPrice","current_price","salePrice","sale_price",
                 "sellingPrice","selling_price","priceValue","price_value"],0)
    if isinstance(v,dict):
        v = first(v,["current","value","amount","price"],0)
    return num(v)

def rating_of(x):
    v = first(x,["rating","stars","starRating","star_rating","customerRating"],0)
    if isinstance(v,dict):
        v = first(v,["average","value","rating"],0)
    return num(v)

def reviews_of(x):
    v = first(x,["reviews","reviewCount","review_count","ratingsTotal","ratings_total",
                 "orders","orderCount","order_count","customerReviewCount"],0)
    if isinstance(v,dict):
        v = first(v,["reviewsCount","count","total"],0)
    return integer(v)

def desc_of(x):
    return str(first(x,["description","productDescription","product_description",
                         "features","bullets","category"],""))

def url_of(x):
    return first(x,["url","productUrl","product_url","detailPageURL","link"],None)

def image_of(x):
    return first(x,["image","imageUrl","image_url","thumbnail","thumbnailUrl"],None)

def classify(text):
    t = norm(text)
    scores = {k:sum(1 for kw in kws if norm(kw) in t) for k,kws in CATEGORIES.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else None

def hard_reject(name, desc):
    t = norm(name+" "+desc)
    bad = [
        "electronic","battery","bluetooth","usb","smart device",
        "glass vase","glassware","fragile","edible","food supplement",
        "drink","beverage","seasonal","christmas","halloween",
        "valentine","easter","furniture","sofa","cabinet"
    ]
    return any(x in t for x in bad)

def sim(a,b):
    aa=set(norm(a).split()); bb=set(norm(b).split())
    overlap=len(aa&bb)/max(1,len(aa|bb))
    return max(SequenceMatcher(None,norm(a),norm(b)).ratio(), overlap)

def score_product(selling,cogs,rating,reviews,name,desc,category):
    markup=selling/cogs if cogs else 0
    t=norm(name+" "+desc)
    def s(cond,yes=10,no=4): return yes if cond else no
    parts={
        "wow_factor":s(len(name)>=18 and len(set(norm(name).split()))>=4,9,5),
        "price":10 if 50<=selling<=80 else 8 if selling>=40 else 0,
        "cogs":10 if 10<=cogs<=35 else 7 if cogs>0 else 0,
        "markup":10 if markup>=4 else 9 if markup>=3 else 0,
        "perceived_value":9 if selling>=50 else 7,
        "pain_point":8 if any(w in t for w in ["organizer","storage","space","clean","clutter","holder","rack","saving","utility"]) else 6,
        "daily_use":8 if any(w in t for w in ["kitchen","storage","organizer","clean","holder","drawer","pantry","sink"]) else 6,
        "golden_customer":8,
        "lightweight":9 if not any(w in t for w in ["heavy","large furniture","sofa","cabinet"]) else 2,
        "safe_shipping":9 if not any(w in t for w in ["glass","ceramic","mirror","crystal"]) else 2,
        "evergreen":9 if not any(w in t for w in ["christmas","halloween","valentine","easter","seasonal"]) else 2,
        "quality":min(10,max(4,4+rating*1.5)),
        "proven_seller":10 if reviews>=1000 else 9 if reviews>=500 else 8 if reviews>=50 else 0,
        "content_potential":9 if any(w in t for w in ["organizer","storage","decor","rack","holder","space","clean","before","after"]) else 7,
    }
    total=round(sum(parts[k]*WEIGHTS[k]/10 for k in WEIGHTS),1)
    return total,parts

def verdict(s):
    return "EXCEPTIONAL" if s>=90 else "STRONG WINNER" if s>=80 else "TEST" if s>=70 else "WATCH" if s>=60 else "REJECT"

def save_product(p):
    c=db(); t=now()
    f=hashlib.sha256((norm(p.get("name",""))+"|"+norm(p.get("url") or "")).encode()).hexdigest()
    c.execute("""INSERT INTO products(fingerprint,data,created_at,updated_at)
        VALUES(?,?,?,?) ON CONFLICT(fingerprint) DO UPDATE SET
        data=excluded.data,updated_at=excluded.updated_at""",
        (f,json.dumps(p,ensure_ascii=False),t,t))
    c.commit(); c.close()

def products(limit=50,category=None):
    c=db()
    rows=c.execute("""SELECT data FROM products
        ORDER BY json_extract(data,'$.score') DESC, updated_at DESC LIMIT ?""",
        (max(1,min(limit,200)),)).fetchall()
    c.close()
    out=[json.loads(r[0]) for r in rows]
    return [p for p in out if not category or p.get("category_key")==category]

async def rapid_get(url, host, key, params):
    headers={"x-rapidapi-key":key,"x-rapidapi-host":host}
    async with httpx.AsyncClient(timeout=45) as c:
        r=await c.get(url,headers=headers,params=params)
        if r.status_code>=400:
            raise RuntimeError(f"RapidAPI HTTP {r.status_code}: {r.text[:500]}")
        return r.json()

def extract_list(body):
    if isinstance(body,list): return body
    if not isinstance(body,dict): return []
    for key in ["products","items","results","data"]:
        v=body.get(key)
        if isinstance(v,list): return v
        if isinstance(v,dict):
            for k in ["products","items","results"]:
                if isinstance(v.get(k),list): return v[k]
    for v in body.values():
        if isinstance(v,list) and v and isinstance(v[0],dict):
            return v
    return []

async def run_rapidapi_amazon(queries):
    key=os.getenv("RAPIDAPI_KEY")
    host=os.getenv("RAPIDAPI_AMAZON_HOST")
    url=os.getenv("RAPIDAPI_AMAZON_URL")
    if not key or not host:
        return [], "not_configured"
    if not url:
        url=f"https://{host}/amazon-serp.php"
    out=[]
    for q in queries:
        data=await rapid_get(url,host,key,{"query":q})
        body=data.get("body",data) if isinstance(data,dict) else data
        out.extend(extract_list(body))
    return out, None

async def run_rapidapi_aliexpress(queries):
    key=os.getenv("RAPIDAPI_KEY")
    host=os.getenv("RAPIDAPI_ALIEXPRESS_HOST","aliexpress-api2.p.rapidapi.com")
    url=os.getenv("RAPIDAPI_ALIEXPRESS_URL","https://aliexpress-api2.p.rapidapi.com/search")
    if not key:
        return [], "not_configured"
    out=[]
    for q in queries:
        data=await rapid_get(url,host,key,{"SearchText":q})
        body=data.get("body",data) if isinstance(data,dict) else data
        for x in extract_list(body):
            if isinstance(x,dict):
                y=dict(x)
                if isinstance(y.get("price"),dict):
                    y["price"]=first(y["price"],["current","value","amount"],0)
                if isinstance(y.get("customerReview"),dict):
                    y["rating"]=first(y["customerReview"],["average"],0)
                    y["reviews"]=first(y["customerReview"],["reviewsCount"],0)
                if "soldCount" in y and "orders" not in y:
                    y["orders"]=y["soldCount"]
                out.append(y)
    return out, None

async def run_actor(actor,token,payload):
    url=f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items"
    async with httpx.AsyncClient(timeout=360) as c:
        r=await c.post(url,params={"token":token,"format":"json","clean":"true"},json=payload)
        if r.status_code>=400:
            raise RuntimeError(f"Apify actor {actor} HTTP {r.status_code}: {r.text[:500]}")
        data=r.json()
        if not isinstance(data,list): raise RuntimeError("Apify returned non-list data")
        return data

async def execute_hunt(category, req):
    if category not in CATEGORIES:
        raise ValueError("Only kitchen_utility and home_decor are allowed")
    queries=[req.keyword] if req.keyword else CATEGORIES[category][:5]
    queries=[q for q in queries if q]

    c=db()
    cur=c.execute("INSERT INTO hunts(category,status,started_at) VALUES(?,?,?)",
                  (category,"running",now()))
    run_id=cur.lastrowid; c.commit(); c.close()

    diag={"category":category,"queries":queries,"sources":{},"filters":{
        "min_price":req.min_price,"max_cogs":req.max_cogs,
        "min_markup":req.min_markup,"min_rating":req.min_rating,
        "min_reviews":req.min_reviews,"min_score":req.min_score
    }}

    try:
        demand=[]; supplier=[]
        # RapidAPI is primary when configured.
        try:
            demand,err=await run_rapidapi_amazon(queries)
            diag["sources"]["rapidapi_amazon"]={"items":len(demand),"error":err}
        except Exception as e:
            diag["sources"]["rapidapi_amazon"]={"items":0,"error":str(e)}

        # Optional Apify fallback/extra demand source.
        token=os.getenv("APIFY_API_TOKEN")
        demand_actor=os.getenv("APIFY_DEMAND_ACTOR_ID")
        if not demand and token and demand_actor:
            try:
                demand_input={"searchQueries":queries,"marketplace":req.marketplace,
                              "maxProductsPerSearch":max(5,min(req.max_products,50)),
                              "maxSearchPages":max(1,min(req.amazon_pages,5))}
                demand=await run_actor(demand_actor,token,demand_input)
                diag["sources"]["apify_amazon"]={"items":len(demand),"error":None}
            except Exception as e:
                diag["sources"]["apify_amazon"]={"items":0,"error":str(e)}

        try:
            supplier,err=await run_rapidapi_aliexpress(queries)
            diag["sources"]["rapidapi_aliexpress"]={"items":len(supplier),"error":err}
        except Exception as e:
            diag["sources"]["rapidapi_aliexpress"]={"items":0,"error":str(e)}

        supplier_actor=os.getenv("APIFY_SUPPLIER_ACTOR_ID")
        if not supplier and token and supplier_actor:
            try:
                supplier_input={"mode":"search","searchQuery":" ".join(queries[:2]),
                    "region":"com","currency":"USD","language":"en_US",
                    "sortBy":"orders_desc","priceMin":0,"priceMax":int(req.max_cogs),
                    "ratingMin":req.min_rating,"shipTo":req.supplier_ship_to,
                    "maxItems":max(20,min(req.max_products*5,100)),
                    "maxPages":max(1,min(req.supplier_pages,10))}
                supplier=await run_actor(supplier_actor,token,supplier_input)
                diag["sources"]["apify_aliexpress"]={"items":len(supplier),"error":None}
            except Exception as e:
                diag["sources"]["apify_aliexpress"]={"items":0,"error":str(e)}

        diag["raw_demand_items"]=len(demand)
        diag["raw_supplier_items"]=len(supplier)

        suppliers=[]
        for x in supplier:
            n=name_of(x); cost=price_of(x)
            if n and cost>0: suppliers.append((n,cost,x))

        counts={k:0 for k in [
            "missing_basic","hard_reject","wrong_category","price_rating_reviews",
            "no_supplier_match","bad_cogs_markup","low_score","accepted"
        ]}
        accepted=[]

        for i,x in enumerate(demand):
            n=name_of(x); sell=price_of(x); rating=rating_of(x); reviews=reviews_of(x); desc=desc_of(x)
            if not n or sell<=0:
                counts["missing_basic"]+=1; continue
            if hard_reject(n,desc):
                counts["hard_reject"]+=1; continue
            cat=classify(n+" "+desc+" "+category)
            if cat!=category:
                counts["wrong_category"]+=1; continue
            if sell<req.min_price or rating<req.min_rating or reviews<req.min_reviews:
                counts["price_rating_reviews"]+=1; continue

            best=None; bestsim=0
            for sn,sc,sx in suppliers:
                z=sim(n,sn)
                if z>bestsim: bestsim=z; best=(sn,sc,sx)
            if not best or bestsim<0.38:
                counts["no_supplier_match"]+=1; continue

            supplier_name,cogs,sx=best
            if cogs<=0 or cogs>req.max_cogs or sell/cogs<req.min_markup:
                counts["bad_cogs_markup"]+=1; continue

            score,breakdown=score_product(sell,cogs,rating,reviews,n,desc,category)
            if score<req.min_score:
                counts["low_score"]+=1; continue

            p={
                "id":str(first(x,["asin","id","sku","productId"],f"product-{i}")),
                "name":n,"category_key":category,
                "category":"Kitchen Utility" if category=="kitchen_utility" else "Home Décor",
                "selling_price":round(sell,2),"cost_of_goods":round(cogs,2),
                "markup":round(sell/cogs,2),"rating":rating,"reviews":reviews,
                "score":score,"winner_score":score,"verdict":verdict(score),
                "supplier_name":supplier_name,"supplier_match_score":round(bestsim,3),
                "image":image_of(x),"url":url_of(x),
                "supplier_url":url_of(sx),"breakdown":breakdown,
                "discovered_at":now()
            }
            save_product(p); accepted.append(p); counts["accepted"]+=1

        diag["pipeline"]=counts
        diag["accepted"]=len(accepted)

        c=db()
        c.execute("""UPDATE hunts SET status=?,finished_at=?,demand_items=?,
            supplier_items=?,accepted=?,error=? WHERE id=?""",
            ("success",now(),len(demand),len(supplier),len(accepted),
             json.dumps(diag,ensure_ascii=False)[:10000],run_id))
        c.commit(); c.close()

        return {"status":"success","category":category,"run_id":run_id,
                "demand_items":len(demand),"supplier_items":len(supplier),
                "accepted":len(accepted),"rejected":len(demand)-len(accepted),
                "diagnostic":diag,
                "products":sorted(accepted,key=lambda p:p["score"],reverse=True)}
    except Exception as e:
        c=db()
        c.execute("UPDATE hunts SET status=?,finished_at=?,error=? WHERE id=?",
                  ("error",now(),str(e)[:10000],run_id))
        c.commit(); c.close()
        raise

@app.on_event("startup")
def startup():
    db().close()

@app.get("/")
def root():
    return {"service":"Product Hunter API","version":VERSION,
            "categories":list(CATEGORIES),
            "endpoints":["/health","/sources/status","/diagnostic/latest",
                         "/auto-hunt/kitchen_utility","/auto-hunt/home_decor",
                         "/auto-hunt/all","/products/accepted","/products/winners",
                         "/stats","/hunt-history"]}

@app.get("/health")
def health():
    db().close()
    return {
        "status":"ok","version":VERSION,"database":"connected",
        "apify_configured":bool(os.getenv("APIFY_API_TOKEN")),
        "rapidapi_configured":bool(os.getenv("RAPIDAPI_KEY")),
        "rapidapi_amazon_configured":bool(os.getenv("RAPIDAPI_KEY") and os.getenv("RAPIDAPI_AMAZON_HOST")),
        "rapidapi_aliexpress_configured":bool(os.getenv("RAPIDAPI_KEY")),
        "categories_available":list(CATEGORIES)
    }

@app.get("/sources/status")
def sources():
    return {
        "apify":bool(os.getenv("APIFY_API_TOKEN")),
        "demand_actor":bool(os.getenv("APIFY_DEMAND_ACTOR_ID")),
        "supplier_actor":bool(os.getenv("APIFY_SUPPLIER_ACTOR_ID")),
        "rapidapi_key":bool(os.getenv("RAPIDAPI_KEY")),
        "rapidapi_amazon_host":os.getenv("RAPIDAPI_AMAZON_HOST"),
        "rapidapi_amazon_url":os.getenv("RAPIDAPI_AMAZON_URL") or (
            f"https://{os.getenv('RAPIDAPI_AMAZON_HOST')}/amazon-serp.php"
            if os.getenv("RAPIDAPI_AMAZON_HOST") else None),
        "rapidapi_aliexpress_host":os.getenv("RAPIDAPI_ALIEXPRESS_HOST","aliexpress-api2.p.rapidapi.com"),
        "rapidapi_aliexpress_url":os.getenv("RAPIDAPI_ALIEXPRESS_URL","https://aliexpress-api2.p.rapidapi.com/search")
    }

@app.get("/products/accepted")
def accepted(limit:int=50,category:Optional[str]=None):
    p=products(limit,category)
    return {"status":"success","count":len(p),"products":p}

@app.get("/products/winners")
def winners(limit:int=50):
    return {"status":"success","products":[p for p in products(limit) if p.get("score",0)>=80]}

@app.get("/products/category/{category}")
def by_category(category:str,limit:int=50):
    if category not in CATEGORIES:
        return {"status":"error","message":"Only kitchen_utility and home_decor are available"}
    return accepted(limit,category)

@app.get("/stats")
def stats():
    ps=products(200)
    return {"products":len(ps),
            "exceptional":sum(p.get("score",0)>=90 for p in ps),
            "strong_winner":sum(80<=p.get("score",0)<90 for p in ps),
            "test":sum(70<=p.get("score",0)<80 for p in ps),
            "watch":sum(60<=p.get("score",0)<70 for p in ps),
            "kitchen_utility":sum(p.get("category_key")=="kitchen_utility" for p in ps),
            "home_decor":sum(p.get("category_key")=="home_decor" for p in ps)}

@app.get("/hunt-history")
def history(limit:int=20):
    c=db()
    rows=[dict(r) for r in c.execute(
        "SELECT * FROM hunts ORDER BY id DESC LIMIT ?",
        (max(1,min(limit,100)),)).fetchall()]
    c.close()
    return {"runs":rows}

@app.get("/diagnostic/latest")
def diagnostic_latest():
    c=db()
    row=c.execute("SELECT * FROM hunts ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    if not row: return {"status":"no_hunts"}
    out=dict(row)
    try: out["diagnostic"]=json.loads(out.get("error") or "{}")
    except: out["diagnostic"]=out.get("error")
    return out

@app.get("/verdicts")
def verdicts():
    ps=products(200)
    return {v:sum(p.get("verdict")==v for p in ps)
            for v in ["EXCEPTIONAL","STRONG WINNER","TEST","WATCH"]}

@app.post("/auto-hunt/{category}")
async def auto_hunt(category:str, background_tasks:BackgroundTasks):
    if category not in CATEGORIES and category!="all":
        return {"status":"error","message":"Category must be kitchen_utility, home_decor or all"}
    cats=list(CATEGORIES) if category=="all" else [category]
    for cat in cats:
        background_tasks.add_task(_bg_hunt,cat)
    return {"status":"success","message":"Hunt started","categories":cats}

async def _bg_hunt(cat):
    try:
        await execute_hunt(cat,HuntRequest(category=cat,min_score=60,min_price=40,
            max_price=150,max_cogs=35,min_markup=3,min_rating=4,min_reviews=50))
    except Exception as e:
        print(f"Hunt {cat} failed: {e}")

@app.post("/admin/hunt-now/{category}")
async def hunt_now(category:str):
    if category not in CATEGORIES:
        return {"status":"error","message":"Invalid category"}
    try:
        return await execute_hunt(category,HuntRequest(category=category,min_score=60,
            min_price=40,max_price=150,max_cogs=35,min_markup=3,min_rating=4,min_reviews=50))
    except Exception as e:
        return {"status":"error","message":str(e)}

@app.post("/hunt")
async def legacy_hunt(req:HuntRequest):
    cat=req.category if req.category in CATEGORIES else classify(req.keyword or "")
    if cat not in CATEGORIES: cat="kitchen_utility"
    try: return await execute_hunt(cat,req)
    except Exception as e: return {"status":"error","message":str(e)}
