# schemas/run.py
from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class RunResponse(BaseModel):
    id:                int
    run_code:          str
    study_id:          Optional[str]
    status:            str
    total_issues:      int
    high_issues:       int
    medium_issues:     int
    low_issues:        int
    subjects_impacted: int
    sites_impacted:    int
    uploaded_files:    list[dict]
    error_message:     Optional[str]
    started_at:        Optional[datetime]
    finished_at:       Optional[datetime]
    created_by:        Optional[str]
    created_at:        datetime

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_run(cls, run) -> "RunResponse":
        import json
        return cls(
            id=run.id,
            run_code=run.run_code,
            study_id=run.study_id,
            status=run.status,
            total_issues=run.total_issues or 0,
            high_issues=run.high_issues or 0,
            medium_issues=run.medium_issues or 0,
            low_issues=run.low_issues or 0,
            subjects_impacted=run.subjects_impacted or 0,
            sites_impacted=run.sites_impacted or 0,
            uploaded_files=json.loads(run.uploaded_files or "[]"),
            error_message=run.error_message,
            started_at=run.started_at,
            finished_at=run.finished_at,
            created_by=run.created_by,
            created_at=run.created_at,
        )


class RunSummaryResponse(BaseModel):
    """대시보드 KPI + 차트 데이터."""
    kpi: dict
    issues_by_rule_code: list[dict]
    issues_by_site:      list[dict]
    issues_by_severity:  list[dict]
    issues_over_time:    list[dict]
