from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import asyncpg, os, logging, asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI(title="Delta-RO API")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://delta:deltapass@postgres-service:5432/deltadb")
pool = None

@app.on_event("startup")
async def startup():
    global pool
    for attempt in range(10):
        try:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
            async with pool.acquire() as conn:
                await conn.execute("""CREATE TABLE IF NOT EXISTS scores (
                    id SERIAL PRIMARY KEY, player_name VARCHAR(50) NOT NULL,
                    score INTEGER NOT NULL, created_at TIMESTAMP DEFAULT NOW())""")
            logger.info("DB ready."); break
        except Exception as e:
            logger.warning(f"DB attempt {attempt+1}/10: {e}"); await asyncio.sleep(3)

@app.on_event("shutdown")
async def shutdown():
    if pool: await pool.close()

class ScoreIn(BaseModel):
    player_name: str
    score: int

class ScoreOut(BaseModel):
    id: int; player_name: str; score: int

@app.get("/health")
async def health(): return {"status": "ok"}

@app.get("/scores", response_model=list[ScoreOut])
async def get_scores(limit: int = 10):
    if not pool: raise HTTPException(503, "DB unavailable")
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, player_name, score FROM scores ORDER BY score DESC LIMIT $1", min(limit, 100))
    return [dict(r) for r in rows]

@app.post("/scores", response_model=ScoreOut, status_code=201)
async def post_score(data: ScoreIn):
    if not pool: raise HTTPException(503, "DB unavailable")
    if data.score < 0 or data.score > 9999999: raise HTTPException(400, "Invalid score")
    name = data.player_name.strip()[:50] or "OPERATIVE"
    async with pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO scores (player_name, score) VALUES ($1, $2) RETURNING id, player_name, score", name, data.score)
    return dict(row)

app.mount("/", StaticFiles(directory="/app/frontend", html=True), name="frontend")
