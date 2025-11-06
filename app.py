import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from databases import Database
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

app = FastAPI(title="Bricktickler → Dossier", version="1.0.0")

# One Database instance for the whole app
database = Database(DATABASE_URL)

@app.on_event("startup")
async def startup():
    await database.connect()
    app.state.db = database   # <-- make it available to routers

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# Static pages
app.mount("/static", StaticFiles(directory="static"), name="static")

# Routers
from routers.exo import exo_router
app.include_router(exo_router)
