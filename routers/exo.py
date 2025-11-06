# routers/exo.py
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import os, hmac, hashlib, uuid, json
from sqlalchemy import text
from databases import Database

exo_router = APIRouter(prefix="/exo", tags=["exo"])

# --- Security helpers ---
HMAC_SECRET = os.getenv("EXO_BUILDER_HMAC", "CHANGE_ME")
def verify_hmac(raw: bytes, sig: str) -> bool:
    mac = hmac.new(HMAC_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, sig or "")
def get_db(request: Request):
    db = getattr(request.app.state, "db", None)
    if not db:
        raise HTTPException(500, "DB not initialized")
    return db

def manager_auth(_: Request):
    return True

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
db: Database = Database(DATABASE_URL)

# Optional: dependency for manager auth (JWT or session)
def manager_auth(request: Request):
    # Reuse your BRidge auth (bearer/session). Raise 401 if not manager.
    return True

# --- Schemas ---
class Builder(BaseModel):
    id: Optional[uuid.UUID] = None
    name: str
    org_id: Optional[uuid.UUID] = None

class Location(BaseModel):
    label: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class Timestamps(BaseModel):
    built_at: datetime
    cured_at: Optional[datetime] = None
    installed_at: Optional[datetime] = None

class Materials(BaseModel):
    straw_bales: int
    straw_ft3: float
    hemp_hurd_bags: int
    hemp_hurd_ft3: float
    rubber_mulch_bags: int
    rubber_mulch_ft3: float
    mycelium_blocks: int
    mycelium_ft3: float
    type_s_lime_bags: int
    type_s_lime_ft3: float
    water_gal: float
    air_expansion_pct: Optional[str] = None
    hardware_cloth_sqft: Optional[str] = None
    rebar: dict

class Media(BaseModel):
    photo_urls: Optional[List[str]] = None
    doc_urls: Optional[List[str]] = None

class ExoPayload(BaseModel):
    idempotency_key: str
    external_ref: Optional[str] = None
    builder: Builder
    location: Optional[Location] = None
    timestamps: Timestamps
    status: str = Field(pattern="^(poured|curing|cured|installed|damaged|retired)$")
    metrics: dict
    method: Optional[str] = None
    core_fill: Optional[dict] = None
    materials: Materials
    batches: Optional[List[dict]] = None
    structure_notes: Optional[str] = None
    media: Optional[Media] = None
    signature: Optional[str] = None
    qr_slug: Optional[str] = None

# --- 2.1 Builder intake (HMAC + idempotency) ---
@exo_router.post(
    "/intake",
    summary="Builder intake (staging)",
    description="Builders POST signed payloads. Idempotent on idempotency_key. Requires X-Signature HMAC.",
    status_code=201,
)
async def intake_exo_blocks(
    request: Request,
    x_signature: str = Header(..., alias="X-Signature")
):
    raw = await request.body()
    if not verify_hmac(raw, x_signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        payload = json.loads(raw)
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    items = payload if isinstance(payload, list) else [payload]
    created = []

    async with db.transaction():
        for it in items:
            data = ExoPayload(**it)  # validation
            # upsert-like idempotency: skip if exists
            exists = await db.fetch_one(
                "SELECT id FROM exo_blocks_staging WHERE idempotency_key=:k",
                {"k": data.idempotency_key},
            )
            if exists:
                continue

            bid = uuid.uuid4()
            created.append(str(bid))
            q = text("""
              INSERT INTO exo_blocks_staging(
                id,idempotency_key,external_ref,builder_id,builder_name,org_id,
                location_label,latitude,longitude,
                built_at,cured_at,installed_at,status,
                co2_offset_lbs,volume_ft3,height_in,total_cost_usd,
                method,core_fill,structure_notes,materials,batches,
                photo_urls,doc_urls,signature,qr_slug,created_at,updated_at
              ) VALUES(
                :id,:idem,:ext,:builder_id,:builder_name,:org_id,
                :loc,:lat,:lng,
                :built,:cured,:installed,:status,
                :co2,:vol,:height,:cost,
                :method,:core_fill,:notes,:materials,:batches,
                :photos,:docs,:signature,:qr,now(),now()
              )
            """)
            m = data.materials.model_dump()
            await db.execute(q, {
                "id": bid,
                "idem": data.idempotency_key,
                "ext": data.external_ref,
                "builder_id": str(data.builder.id) if data.builder.id else None,
                "builder_name": data.builder.name,
                "org_id": str(data.builder.org_id) if data.builder.org_id else None,
                "loc": (data.location.label if data.location else None),
                "lat": (data.location.latitude if data.location else None),
                "lng": (data.location.longitude if data.location else None),
                "built": data.timestamps.built_at,
                "cured": data.timestamps.cured_at,
                "installed": data.timestamps.installed_at,
                "status": data.status,
                "co2": float(data.metrics.get("co2_offset_lbs", 0)),
                "vol": data.metrics.get("volume_ft3"),
                "height": data.metrics.get("height_in"),
                "cost": data.metrics.get("total_cost_usd"),
                "method": data.method,
                "core_fill": json.dumps(data.core_fill),
                "notes": data.structure_notes,
                "materials": json.dumps(m),
                "batches": json.dumps(data.batches),
                "photos": json.dumps((data.media.photo_urls if data.media else None)),
                "docs": json.dumps((data.media.doc_urls if data.media else None)),
                "signature": data.signature,
                "qr": data.qr_slug,
            })

            await db.execute(
              "INSERT INTO exo_block_events(id,block_id,table_name,event_type,payload) "
              "VALUES(:id,:bid,'staging','INGEST',:payload)",
              {"id": uuid.uuid4(), "bid": bid, "payload": json.dumps({"status": data.status})}
            )

    return {"created": created}

# --- 2.2 Manager: list staging with filters + pagination ---
@exo_router.get(
    "/staging",
    summary="List staging items (manager)",
    description="Filter by review_status, external_ref; paginate with limit/offset.",
)
async def list_staging(
    review_status: Optional[str] = Query(None, pattern="^(PENDING|APPROVED|REJECTED)$"),
    external_ref: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _=Depends(manager_auth),
):
    where = ["1=1"]
    params = {}
    if review_status:
        where.append("review_status=:rs"); params["rs"] = review_status
    if external_ref:
        where.append("external_ref ILIKE :er"); params["er"] = f"%{external_ref}%"

    rows = await db.fetch_all(
        f"SELECT * FROM exo_blocks_staging WHERE {' AND '.join(where)} "
        "ORDER BY created_at DESC LIMIT :lim OFFSET :off",
        {**params, "lim": limit, "off": offset}
    )
    total = await db.fetch_val(
        f"SELECT count(*) FROM exo_blocks_staging WHERE {' AND '.join(where)}", params
    )
    return {"total": total, "limit": limit, "offset": offset, "items": [dict(r) for r in rows]}

# --- 2.3 Manager: approve / reject / status tweak ---
class ReviewAction(BaseModel):
    action: str = Field(pattern="^(APPROVE|REJECT|MARK_CURED)$")
    reviewer_id: Optional[uuid.UUID] = None
    reviewer_name: Optional[str] = None

@exo_router.patch(
    "/staging/{id}",
    summary="Review a staging item",
    description="Approve, reject, or mark cured. Approval promotes to final table.",
)
async def review_item(
    id: uuid.UUID,
    body: ReviewAction,
    _=Depends(manager_auth),
):
    row = await db.fetch_one("SELECT * FROM exo_blocks_staging WHERE id=:id", {"id": str(id)})
    if not row:
        raise HTTPException(404, "Not found")

    if body.action == "REJECT":
        await db.execute(
            "UPDATE exo_blocks_staging SET review_status='REJECTED', reviewer_id=:rid, reviewer_name=:rname, reviewed_at=now(), updated_at=now() WHERE id=:id",
            {"rid": str(body.reviewer_id) if body.reviewer_id else None, "rname": body.reviewer_name, "id": str(id)}
        )
        await db.execute("INSERT INTO exo_block_events(id,block_id,table_name,event_type) VALUES(:id,:bid,'staging','REJECT')",
                         {"id": uuid.uuid4(), "bid": str(id)})
        return {"ok": True, "state": "REJECTED"}

    if body.action == "MARK_CURED":
        await db.execute(
            "UPDATE exo_blocks_staging SET status='cured', cured_at=now(), updated_at=now() WHERE id=:id",
            {"id": str(id)}
        )
        await db.execute("INSERT INTO exo_block_events(id,block_id,table_name,event_type) VALUES(:id,:bid,'staging','STATUS_CHANGE')",
                         {"id": uuid.uuid4(), "bid": str(id)})
        return {"ok": True, "state": "cured"}

    # APPROVE => promote to final
    async with db.transaction():
        # insert into final
        await db.execute(text("""
          INSERT INTO exo_blocks(
            id, external_ref, builder_id, builder_name, org_id, location_label, latitude, longitude,
            built_at, cured_at, installed_at, status,
            co2_offset_lbs, volume_ft3, height_in, total_cost_usd,
            method, core_fill, structure_notes, materials, batches,
            photo_urls, doc_urls, signature, qr_slug, created_at, updated_at
          )
          SELECT
            id, external_ref, builder_id, builder_name, org_id, location_label, latitude, longitude,
            built_at, cured_at, installed_at, status,
            co2_offset_lbs, volume_ft3, height_in, total_cost_usd,
            method, core_fill, structure_notes, materials, batches,
            photo_urls, doc_urls, signature, qr_slug, now(), now()
          FROM exo_blocks_staging WHERE id=:id
          ON CONFLICT (external_ref) DO NOTHING
        """), {"id": str(id)})

        await db.execute(
            "UPDATE exo_blocks_staging SET review_status='APPROVED', reviewer_id=:rid, reviewer_name=:rname, reviewed_at=now(), updated_at=now() WHERE id=:id",
            {"rid": str(body.reviewer_id) if body.reviewer_id else None, "rname": body.reviewer_name, "id": str(id)}
        )
        await db.execute("INSERT INTO exo_block_events(id,block_id,table_name,event_type) VALUES(:id,:bid,'final','APPROVE')",
                         {"id": uuid.uuid4(), "bid": str(id)})

    return {"ok": True, "state": "APPROVED"}

# --- 2.4 Export / push approved to Dossier dump (JSON download or push) ---
@exo_router.get(
    "/staging/export",
    summary="Export APPROVED items as dossier_dump.json",
    description="Returns a JSON array compatible with /ingest/exo-blocks.",
)
async def export_approved(_=Depends(manager_auth)):
    rows = await db.fetch_all("SELECT * FROM exo_blocks_staging WHERE review_status='APPROVED' ORDER BY reviewed_at DESC")
    items = []
    for r in rows:
        items.append({
            "idempotency_key": r["idempotency_key"],
            "external_ref": r["external_ref"],
            "builder": {"id": r["builder_id"], "name": r["builder_name"], "org_id": r["org_id"]},
            "location": {"label": r["location_label"], "latitude": r["latitude"], "longitude": r["longitude"]},
            "timestamps": {"built_at": r["built_at"], "cured_at": r["cured_at"], "installed_at": r["installed_at"]},
            "status": r["status"],
            "metrics": {"co2_offset_lbs": r["co2_offset_lbs"], "volume_ft3": r["volume_ft3"], "height_in": r["height_in"], "total_cost_usd": r["total_cost_usd"]},
            "method": r["method"],
            "core_fill": r["core_fill"],
            "materials": r["materials"],
            "batches": r["batches"],
            "structure_notes": r["structure_notes"],
            "media": {"photo_urls": r["photo_urls"], "doc_urls": r["doc_urls"]},
            "signature": r["signature"],
            "qr_slug": r["qr_slug"],
        })
    return items
