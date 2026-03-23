# models.py
# Clinical Data Cleaning Assistant — SQLAlchemy ORM Models
# ============================================================
# pip install sqlalchemy

import json
from datetime import datetime
from typing import Optional, List, Any

from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime,
    ForeignKey, CheckConstraint, Index, event
)
from sqlalchemy.orm import DeclarativeBase, relationship, Session


# ------------------------------------------------------------
# Base
# ------------------------------------------------------------
class Base(DeclarativeBase):
    pass


# ------------------------------------------------------------
# 헬퍼: JSON 컬럼을 Python dict/list로 쉽게 읽고 쓰기
# ------------------------------------------------------------
class JSONColumn:
    """conditions, join_keys 등 JSON 컬럼 직렬화/역직렬화 헬퍼."""

    @staticmethod
    def dump(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False)

    @staticmethod
    def load(value: Optional[str]) -> Any:
        if value is None:
            return None
        return json.loads(value)


# ------------------------------------------------------------
# 1. Rule
# ------------------------------------------------------------
class Rule(Base):
    __tablename__ = "rules"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    rule_code        = Column(String(20),  nullable=False, unique=True)   # "RULE-001"
    name             = Column(String(200), nullable=False)
    description      = Column(Text)

    rule_type        = Column(String(30),  nullable=False)
    # COMPARE | REQUIRED | PROHIBITED | DATE_ORDER | DATE_WINDOW |
    # TIME_WINDOW | CODELIST | RANGE | VISIT_COMPLETE | VISIT_ORDER | CROSS_CRF

    datasets         = Column(Text, nullable=False)   # JSON: ["ECG","DOV"]
    conditions       = Column(Text, nullable=False)   # JSON: 상세 조건 구조
    join_keys        = Column(Text, nullable=False, default='["SUBJID"]')  # JSON

    severity         = Column(String(10), nullable=False, default="medium")
    message_template = Column(Text, nullable=False)

    status           = Column(String(10), nullable=False, default="active")
    created_by       = Column(String(100))
    created_at       = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at       = Column(DateTime, nullable=False, default=datetime.utcnow,
                              onupdate=datetime.utcnow)
    version          = Column(Integer, nullable=False, default=1)

    __table_args__ = (
        CheckConstraint("severity IN ('high','medium','low')",  name="ck_rule_severity"),
        CheckConstraint("status   IN ('active','inactive')",    name="ck_rule_status"),
        Index("idx_rules_status",    "status"),
        Index("idx_rules_rule_type", "rule_type"),
    )

    # relationships
    issues    = relationship("Issue",          back_populates="rule")
    audit_log = relationship("RuleAuditLog",   back_populates="rule")

    # ── JSON 편의 프로퍼티 ──────────────────────────────────

    @property
    def conditions_dict(self) -> dict:
        return JSONColumn.load(self.conditions) or {}

    @conditions_dict.setter
    def conditions_dict(self, value: dict):
        self.conditions = JSONColumn.dump(value)

    @property
    def datasets_list(self) -> list:
        return JSONColumn.load(self.datasets) or []

    @datasets_list.setter
    def datasets_list(self, value: list):
        self.datasets = JSONColumn.dump(value)

    @property
    def join_keys_list(self) -> list:
        return JSONColumn.load(self.join_keys) or ["SUBJID"]

    @join_keys_list.setter
    def join_keys_list(self, value: list):
        self.join_keys = JSONColumn.dump(value)

    def __repr__(self):
        return f"<Rule {self.rule_code} [{self.rule_type}] {self.status}>"


# ------------------------------------------------------------
# 2. Run
# ------------------------------------------------------------
class Run(Base):
    __tablename__ = "runs"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    run_code         = Column(String(30),  nullable=False, unique=True)   # "RUN-20250714-001"
    study_id         = Column(String(50))

    uploaded_files   = Column(Text, nullable=False)   # JSON: [{crf, filename, rows}, ...]
    applied_rule_ids = Column(Text)                   # JSON: [1,2,3] or null → 전체 적용

    status           = Column(String(15), nullable=False, default="pending")
    # pending | running | done | failed

    total_issues     = Column(Integer, default=0)
    high_issues      = Column(Integer, default=0)
    medium_issues    = Column(Integer, default=0)
    low_issues       = Column(Integer, default=0)
    subjects_impacted = Column(Integer, default=0)
    sites_impacted   = Column(Integer, default=0)

    error_message    = Column(Text)

    started_at       = Column(DateTime)
    finished_at      = Column(DateTime)
    created_by       = Column(String(100))
    created_at       = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending','running','done','failed')",
            name="ck_run_status"
        ),
        Index("idx_runs_status",     "status"),
        Index("idx_runs_study_id",   "study_id"),
        Index("idx_runs_created_at", "created_at"),
    )

    # relationships
    issues      = relationship("Issue",      back_populates="run",
                               cascade="all, delete-orphan")
    crf_uploads = relationship("CRFUpload",  back_populates="run",
                               cascade="all, delete-orphan")

    # ── JSON 편의 프로퍼티 ──────────────────────────────────

    @property
    def uploaded_files_list(self) -> list:
        return JSONColumn.load(self.uploaded_files) or []

    @uploaded_files_list.setter
    def uploaded_files_list(self, value: list):
        self.uploaded_files = JSONColumn.dump(value)

    @property
    def applied_rule_ids_list(self) -> Optional[list]:
        return JSONColumn.load(self.applied_rule_ids)

    @applied_rule_ids_list.setter
    def applied_rule_ids_list(self, value: Optional[list]):
        self.applied_rule_ids = JSONColumn.dump(value) if value is not None else None

    def __repr__(self):
        return f"<Run {self.run_code} [{self.status}] issues={self.total_issues}>"


# ------------------------------------------------------------
# 3. Issue
# ------------------------------------------------------------
class Issue(Base):
    __tablename__ = "issues"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    run_id        = Column(Integer, ForeignKey("runs.id",   ondelete="CASCADE"), nullable=False)
    rule_id       = Column(Integer, ForeignKey("rules.id"), nullable=False)

    subjid        = Column(String(50),  nullable=False)
    siteid        = Column(String(50))
    visit         = Column(String(100))

    left_dataset  = Column(String(30))
    left_field    = Column(String(100))
    left_value    = Column(Text)
    right_dataset = Column(String(30))
    right_field   = Column(String(100))
    right_value   = Column(Text)

    message       = Column(Text, nullable=False)
    severity      = Column(String(10), nullable=False)

    issue_status  = Column(String(20), nullable=False, default="open")
    # open | resolved | acknowledged | waived

    comment       = Column(Text)
    resolved_by   = Column(String(100))
    resolved_at   = Column(DateTime)
    flagged_at    = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        CheckConstraint("severity     IN ('high','medium','low')",              name="ck_issue_severity"),
        CheckConstraint("issue_status IN ('open','resolved','acknowledged','waived')", name="ck_issue_status"),
        Index("idx_issues_run_id",   "run_id"),
        Index("idx_issues_rule_id",  "rule_id"),
        Index("idx_issues_subjid",   "subjid"),
        Index("idx_issues_siteid",   "siteid"),
        Index("idx_issues_severity", "severity"),
        Index("idx_issues_status",   "issue_status"),
    )

    # relationships
    run  = relationship("Run",  back_populates="issues")
    rule = relationship("Rule", back_populates="issues")

    def __repr__(self):
        return f"<Issue run={self.run_id} rule={self.rule_id} subj={self.subjid} [{self.severity}]>"


# ------------------------------------------------------------
# 4. RuleAuditLog
# ------------------------------------------------------------
class RuleAuditLog(Base):
    __tablename__ = "rule_audit_log"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    rule_id         = Column(Integer, ForeignKey("rules.id"), nullable=False)
    action          = Column(String(20), nullable=False)
    # created | updated | deleted | enabled | disabled

    changed_by      = Column(String(100))
    changed_at      = Column(DateTime, nullable=False, default=datetime.utcnow)
    snapshot_before = Column(Text)   # JSON: 변경 전 rule row 스냅샷
    snapshot_after  = Column(Text)   # JSON: 변경 후 rule row 스냅샷

    __table_args__ = (
        CheckConstraint(
            "action IN ('created','updated','deleted','enabled','disabled')",
            name="ck_audit_action"
        ),
        Index("idx_audit_rule_id",    "rule_id"),
        Index("idx_audit_changed_at", "changed_at"),
    )

    rule = relationship("Rule", back_populates="audit_log")

    def __repr__(self):
        return f"<RuleAuditLog rule={self.rule_id} action={self.action}>"


# ------------------------------------------------------------
# 5. CRFUpload
# ------------------------------------------------------------
class CRFUpload(Base):
    __tablename__ = "crf_uploads"

    id                = Column(Integer, primary_key=True, autoincrement=True)
    run_id            = Column(Integer, ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    crf_name          = Column(String(30),  nullable=False)   # "ECG"
    original_filename = Column(String(255), nullable=False)
    row_count         = Column(Integer)
    column_names      = Column(Text)    # JSON: ["SUBJID","SITEID","VISIT",...]
    file_hash         = Column(String(64))   # SHA-256
    uploaded_at       = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_crf_uploads_run_id",   "run_id"),
        Index("idx_crf_uploads_crf_name", "crf_name"),
    )

    run = relationship("Run", back_populates="crf_uploads")

    @property
    def column_names_list(self) -> list:
        return JSONColumn.load(self.column_names) or []

    def __repr__(self):
        return f"<CRFUpload run={self.run_id} crf={self.crf_name} rows={self.row_count}>"


# ============================================================
# DB 초기화 유틸
# ============================================================
def get_engine(db_url: str = "sqlite:///./clinical.db"):
    """
    개발: sqlite:///./clinical.db
    운영: postgresql+psycopg2://user:pass@host/dbname
    """
    engine = create_engine(db_url, echo=False)
    # SQLite 외래키 활성화
    if db_url.startswith("sqlite"):
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(conn, _):
            conn.execute("PRAGMA foreign_keys=ON")
    return engine


def init_db(db_url: str = "sqlite:///./clinical.db"):
    """테이블 생성 (없는 경우에만)."""
    engine = get_engine(db_url)
    Base.metadata.create_all(engine)
    return engine


# ============================================================
# 사용 예시
# ============================================================
if __name__ == "__main__":
    engine = init_db()

    with Session(engine) as session:

        # ── 규칙 생성 예시 ──────────────────────────────────
        rule = Rule(
            rule_code="RULE-001",
            name="ECG date must equal DOV date",
            rule_type="COMPARE",
            severity="high",
            message_template="ECG.ECGD1DAT must equal DOV.DOVDAT for {SUBJID} at {VISIT}",
            status="active",
            created_by="admin",
        )
        # JSON 프로퍼티로 설정
        rule.datasets_list   = ["ECG", "DOV"]
        rule.join_keys_list  = ["SUBJID", "SITEID"]
        rule.conditions_dict = {
            "logic": "AND",
            "filters": [
                {"dataset": "ECG", "field": "VISIT",  "op": "IN",  "value": ["Day 1", "FE Day 1"]},
                {"dataset": "ECG", "field": "ECGYN",  "op": "=",   "value": "Yes"},
            ],
            "compare": {
                "left":  {"dataset": "ECG", "field": "ECGD1DAT"},
                "op":    "=",
                "right": {"dataset": "DOV", "field": "DOVDAT"},
            }
        }
        session.add(rule)

        # ── Run 생성 예시 ────────────────────────────────────
        run = Run(
            run_code="RUN-20250714-001",
            study_id="TRIAL-2025-A",
            status="pending",
            created_by="admin",
        )
        run.uploaded_files_list = [
            {"crf": "ECG", "filename": "ECG_20250714.csv", "rows": 340},
            {"crf": "DOV", "filename": "DOV_20250714.csv", "rows": 120},
        ]
        session.add(run)
        session.commit()

        print("DB 초기화 완료.")
        print(f"  Rule: {rule}")
        print(f"  Run:  {run}")
