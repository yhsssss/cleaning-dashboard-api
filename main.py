# main.py
# Clinical Data Cleaning Assistant — FastAPI Backend
# ============================================================
# 실행: uvicorn main:app --reload
# Swagger UI: http://localhost:8000/docs
# ============================================================

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import init_db
from routers import rules, runs, issues

# ── 앱 생성 ───────────────────────────────────────────────

app = FastAPI(
    title="Clinical Data Cleaning Assistant API",
    description="Validation rule management and CSV-based clinical data cleaning.",
    version="1.0.0",
)

# ── CORS ──────────────────────────────────────────────────
# 개발 중: React dev server (localhost:5173) 허용
# 운영 시: origins 를 실제 도메인으로 교체

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 라우터 등록 ───────────────────────────────────────────

app.include_router(rules.router)
app.include_router(runs.router)
app.include_router(issues.router)

# ── DB 초기화 ─────────────────────────────────────────────

@app.on_event("startup")
def on_startup():
    init_db()
    print("✓ DB initialized")


# ── 헬스체크 ──────────────────────────────────────────────

@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}
