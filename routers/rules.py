# routers/rules.py
import json
import os
import tempfile
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status
from sqlalchemy.orm import Session

from database import get_db
from models import Rule, RuleAuditLog
from schemas.rule import RuleCreate, RuleUpdate, RuleStatusUpdate, RuleResponse, RuleTestResponse
from rule_engine import RuleEngine

router = APIRouter(prefix="/api/rules", tags=["Rules"])


# ── 헬퍼 ──────────────────────────────────────────────────

def _get_rule_or_404(rule_id: int, db: Session) -> Rule:
    rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail=f"Rule {rule_id} not found.")
    return rule


def _snapshot(rule: Rule) -> str:
    return json.dumps({
        "rule_code": rule.rule_code,
        "name": rule.name,
        "rule_type": rule.rule_type,
        "conditions": rule.conditions,
        "severity": rule.severity,
        "status": rule.status,
        "version": rule.version,
    })


def _audit(db: Session, rule: Rule, action: str,
           before: Optional[str] = None, after: Optional[str] = None,
           changed_by: Optional[str] = None):
    db.add(RuleAuditLog(
        rule_id=rule.id,
        action=action,
        changed_by=changed_by,
        snapshot_before=before,
        snapshot_after=after,
    ))


# ── GET /api/rules ─────────────────────────────────────────

@router.get("", response_model=list[RuleResponse])
def list_rules(
    status:    Optional[str] = None,
    rule_type: Optional[str] = None,
    db: Session = Depends(get_db),
):
    q = db.query(Rule)
    if status:
        q = q.filter(Rule.status == status)
    if rule_type:
        q = q.filter(Rule.rule_type == rule_type.upper())
    rules = q.order_by(Rule.rule_code).all()
    return [RuleResponse.from_orm_rule(r) for r in rules]


# ── POST /api/rules ────────────────────────────────────────

@router.post("", response_model=RuleResponse, status_code=status.HTTP_201_CREATED)
def create_rule(body: RuleCreate, db: Session = Depends(get_db)):
    # rule_code 중복 체크
    if db.query(Rule).filter(Rule.rule_code == body.rule_code).first():
        raise HTTPException(status_code=409, detail=f"rule_code '{body.rule_code}' already exists.")

    rule = Rule(
        rule_code=body.rule_code,
        name=body.name,
        description=body.description,
        rule_type=body.rule_type.upper(),
        severity=body.severity,
        message_template=body.message_template,
        status=body.status,
        created_by=body.created_by,
    )
    rule.datasets_list   = body.datasets
    rule.conditions_dict = body.conditions
    rule.join_keys_list  = body.join_keys

    db.add(rule)
    db.flush()   # id 확보

    _audit(db, rule, "created",
           after=_snapshot(rule), changed_by=body.created_by)
    db.commit()
    db.refresh(rule)
    return RuleResponse.from_orm_rule(rule)


# ── GET /api/rules/{id} ────────────────────────────────────

@router.get("/{rule_id}", response_model=RuleResponse)
def get_rule(rule_id: int, db: Session = Depends(get_db)):
    return RuleResponse.from_orm_rule(_get_rule_or_404(rule_id, db))


# ── PUT /api/rules/{id} ────────────────────────────────────

@router.put("/{rule_id}", response_model=RuleResponse)
def update_rule(rule_id: int, body: RuleUpdate, db: Session = Depends(get_db)):
    rule   = _get_rule_or_404(rule_id, db)
    before = _snapshot(rule)

    if body.name             is not None: rule.name             = body.name
    if body.description      is not None: rule.description      = body.description
    if body.rule_type        is not None: rule.rule_type        = body.rule_type.upper()
    if body.severity         is not None: rule.severity         = body.severity
    if body.message_template is not None: rule.message_template = body.message_template
    if body.datasets         is not None: rule.datasets_list    = body.datasets
    if body.conditions       is not None: rule.conditions_dict  = body.conditions
    if body.join_keys        is not None: rule.join_keys_list   = body.join_keys

    rule.version    += 1
    rule.updated_at  = datetime.utcnow()

    _audit(db, rule, "updated", before=before, after=_snapshot(rule))
    db.commit()
    db.refresh(rule)
    return RuleResponse.from_orm_rule(rule)


# ── DELETE /api/rules/{id} ─────────────────────────────────

@router.delete("/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_rule(rule_id: int, db: Session = Depends(get_db)):
    rule   = _get_rule_or_404(rule_id, db)
    before = _snapshot(rule)
    _audit(db, rule, "deleted", before=before)
    db.delete(rule)
    db.commit()


# ── PATCH /api/rules/{id}/status ──────────────────────────

@router.patch("/{rule_id}/status", response_model=RuleResponse)
def toggle_status(rule_id: int, body: RuleStatusUpdate, db: Session = Depends(get_db)):
    rule   = _get_rule_or_404(rule_id, db)
    before = _snapshot(rule)

    if body.status not in ("active", "inactive"):
        raise HTTPException(status_code=422, detail="status must be 'active' or 'inactive'.")

    action      = "enabled" if body.status == "active" else "disabled"
    rule.status = body.status
    rule.updated_at = datetime.utcnow()

    _audit(db, rule, action, before=before, after=_snapshot(rule))
    db.commit()
    db.refresh(rule)
    return RuleResponse.from_orm_rule(rule)


# ── POST /api/rules/{id}/test ──────────────────────────────

@router.post("/{rule_id}/test", response_model=RuleTestResponse)
async def test_rule(
    rule_id: int,
    files: list[UploadFile] = File(...),
    crf_names: str = Form(...),   # JSON: ["ECG","DOV"]
    db: Session = Depends(get_db),
):
    """
    Rule Builder "Test Rule" 기능.
    multipart/form-data 로 CSV 파일들 + CRF 이름 목록을 받아서
    DB 저장 없이 이슈 결과를 미리 반환.

    예: crf_names='["ECG","DOV"]', files=[ECG.csv, DOV.csv]
    """
    rule = _get_rule_or_404(rule_id, db)

    try:
        names = json.loads(crf_names)
    except Exception:
        raise HTTPException(status_code=422, detail="crf_names must be a JSON array.")

    if len(files) != len(names):
        raise HTTPException(
            status_code=422,
            detail=f"Uploaded {len(files)} files but crf_names has {len(names)} entries.",
        )

    # 임시 파일에 저장
    tmp_paths: dict[str, str] = {}
    tmp_dir = tempfile.mkdtemp()
    try:
        for name, upload in zip(names, files):
            tmp_path = os.path.join(tmp_dir, f"{name}.csv")
            content  = await upload.read()
            with open(tmp_path, "wb") as f:
                f.write(content)
            tmp_paths[name.upper()] = tmp_path

        engine = RuleEngine(db)
        result = engine.test_rule(rule, tmp_paths)
    finally:
        for p in tmp_paths.values():
            if os.path.exists(p):
                os.remove(p)
        os.rmdir(tmp_dir)

    return RuleTestResponse(**result)
