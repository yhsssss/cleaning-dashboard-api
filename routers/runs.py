# routers/runs.py
import json
import os
import tempfile
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session

from database import get_db
from models import Run, Issue, Rule
from schemas.run import RunResponse, RunSummaryResponse
from rule_engine import RuleEngine

router = APIRouter(prefix="/api/runs", tags=["Runs"])


# ── POST /api/runs ─────────────────────────────────────────
# CSV 파일들 업로드 + 규칙 실행

@router.post("", response_model=RunResponse)
async def create_run(
    files:     list[UploadFile] = File(...),
    crf_names: str = Form(...),              # JSON: ["ECG","DOV","DM"]
    study_id:  str = Form(default=""),
    rule_ids:  str = Form(default="null"),   # JSON: [1,2,3] or "null"
    created_by: str = Form(default="user"),
    db: Session = Depends(get_db),
):
    """
    multipart/form-data:
      - files:     CSV 파일들 (여러 개)
      - crf_names: JSON 배열 ["ECG","DOV","DM"] — files 순서와 대응
      - study_id:  스터디 ID (optional)
      - rule_ids:  적용할 rule id 목록 JSON, null이면 전체 active 규칙
      - created_by: 실행자
    """
    try:
        names = json.loads(crf_names)
    except Exception:
        raise HTTPException(status_code=422, detail="crf_names must be a JSON array.")

    try:
        r_ids = json.loads(rule_ids)   # None or list[int]
    except Exception:
        r_ids = None

    if len(files) != len(names):
        raise HTTPException(
            status_code=422,
            detail=f"Uploaded {len(files)} files but crf_names has {len(names)} entries.",
        )

    # 임시 디렉토리에 CSV 저장
    tmp_dir   = tempfile.mkdtemp()
    tmp_paths: dict[str, str] = {}
    try:
        for name, upload in zip(names, files):
            tmp_path = os.path.join(tmp_dir, f"{name}.csv")
            content  = await upload.read()
            with open(tmp_path, "wb") as f:
                f.write(content)
            tmp_paths[name.upper()] = tmp_path

        engine = RuleEngine(db)
        run    = engine.execute_run(
            study_id=study_id or "UNKNOWN",
            crf_files=tmp_paths,
            rule_ids=r_ids,
            created_by=created_by,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        for p in tmp_paths.values():
            if os.path.exists(p):
                os.remove(p)
        if os.path.exists(tmp_dir):
            os.rmdir(tmp_dir)

    return RunResponse.from_orm_run(run)


# ── GET /api/runs ──────────────────────────────────────────

@router.get("", response_model=list[RunResponse])
def list_runs(
    study_id: Optional[str] = None,
    status:   Optional[str] = None,
    limit:    int = 20,
    offset:   int = 0,
    db: Session = Depends(get_db),
):
    q = db.query(Run)
    if study_id:
        q = q.filter(Run.study_id == study_id)
    if status:
        q = q.filter(Run.status == status)
    runs = q.order_by(Run.created_at.desc()).offset(offset).limit(limit).all()
    return [RunResponse.from_orm_run(r) for r in runs]


# ── GET /api/runs/{id} ─────────────────────────────────────

@router.get("/{run_id}", response_model=RunResponse)
def get_run(run_id: int, db: Session = Depends(get_db)):
    run = db.query(Run).filter(Run.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found.")
    return RunResponse.from_orm_run(run)


# ── GET /api/runs/{id}/issues ──────────────────────────────

@router.get("/{run_id}/issues")
def get_run_issues(
    run_id:       int,
    severity:     Optional[str] = None,
    issue_status: Optional[str] = None,
    siteid:       Optional[str] = None,
    subjid:       Optional[str] = None,
    limit:        int = 100,
    offset:       int = 0,
    db: Session = Depends(get_db),
):
    run = db.query(Run).filter(Run.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found.")

    q = db.query(Issue).filter(Issue.run_id == run_id)
    if severity:
        q = q.filter(Issue.severity == severity)
    if issue_status:
        q = q.filter(Issue.issue_status == issue_status)
    if siteid:
        q = q.filter(Issue.siteid == siteid)
    if subjid:
        q = q.filter(Issue.subjid.ilike(f"%{subjid}%"))

    total  = q.count()
    issues = q.order_by(Issue.severity, Issue.subjid)\
               .offset(offset).limit(limit).all()

    # rule_code 조인
    rule_map = {r.id: r.rule_code for r in db.query(Rule).all()}

    return {
        "total":  total,
        "offset": offset,
        "limit":  limit,
        "items":  [
            {
                "id":            i.id,
                "run_id":        i.run_id,
                "rule_id":       i.rule_id,
                "rule_code":     rule_map.get(i.rule_id, ""),
                "subjid":        i.subjid,
                "siteid":        i.siteid,
                "visit":         i.visit,
                "left_dataset":  i.left_dataset,
                "left_field":    i.left_field,
                "left_value":    i.left_value,
                "right_dataset": i.right_dataset,
                "right_field":   i.right_field,
                "right_value":   i.right_value,
                "message":       i.message,
                "severity":      i.severity,
                "issue_status":  i.issue_status,
                "comment":       i.comment,
                "flagged_at":    i.flagged_at.isoformat() if i.flagged_at else None,
            }
            for i in issues
        ],
    }


# ── GET /api/runs/{id}/summary ─────────────────────────────
# 대시보드 KPI + 차트 데이터 한 번에 반환

@router.get("/{run_id}/summary", response_model=RunSummaryResponse)
def get_run_summary(run_id: int, db: Session = Depends(get_db)):
    run = db.query(Run).filter(Run.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found.")

    issues = db.query(Issue).filter(Issue.run_id == run_id).all()
    rule_map = {r.id: r.rule_code for r in db.query(Rule).all()}

    # Issues by rule_code
    code_counts: dict[str, int] = {}
    for i in issues:
        code = rule_map.get(i.rule_id, "UNKNOWN")
        code_counts[code] = code_counts.get(code, 0) + 1
    issues_by_rule_code = sorted(
        [{"rule_code": k, "count": v} for k, v in code_counts.items()],
        key=lambda x: -x["count"],
    )

    # Issues by site
    site_counts: dict[str, int] = {}
    for i in issues:
        site = i.siteid or "UNKNOWN"
        site_counts[site] = site_counts.get(site, 0) + 1
    issues_by_site = sorted(
        [{"siteid": k, "count": v} for k, v in site_counts.items()],
        key=lambda x: -x["count"],
    )

    # Issues by severity
    sev_counts = {"high": 0, "medium": 0, "low": 0}
    for i in issues:
        if i.severity in sev_counts:
            sev_counts[i.severity] += 1
    issues_by_severity = [{"severity": k, "count": v} for k, v in sev_counts.items()]

    # Issues over time (by date)
    date_counts: dict[str, int] = {}
    for i in issues:
        d = i.flagged_at.strftime("%Y-%m-%d") if i.flagged_at else "unknown"
        date_counts[d] = date_counts.get(d, 0) + 1
    issues_over_time = sorted(
        [{"date": k, "count": v} for k, v in date_counts.items()],
        key=lambda x: x["date"],
    )

    return RunSummaryResponse(
        kpi={
            "total_issues":      run.total_issues or 0,
            "high_issues":       run.high_issues or 0,
            "medium_issues":     run.medium_issues or 0,
            "low_issues":        run.low_issues or 0,
            "subjects_impacted": run.subjects_impacted or 0,
            "sites_impacted":    run.sites_impacted or 0,
        },
        issues_by_rule_code=issues_by_rule_code,
        issues_by_site=issues_by_site,
        issues_by_severity=issues_by_severity,
        issues_over_time=issues_over_time,
    )
