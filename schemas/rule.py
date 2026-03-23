# schemas/rule.py
from __future__ import annotations
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field


# ── 공통 ──────────────────────────────────────────────────

RULE_TYPES = [
    "COMPARE", "REQUIRED", "PROHIBITED",
    "DATE_ORDER", "DATE_WINDOW", "TIME_WINDOW",
    "CODELIST", "RANGE", "VISIT_COMPLETE", "CROSS_CRF",
]

SEVERITIES   = ["high", "medium", "low"]
RULE_STATUSES = ["active", "inactive"]


# ── Request ───────────────────────────────────────────────

class RuleCreate(BaseModel):
    rule_code:        str         = Field(..., examples=["RULE-001"])
    name:             str
    description:      Optional[str] = None
    rule_type:        str         = Field(..., examples=["COMPARE"])
    datasets:         list[str]   = Field(..., examples=[["ECG", "DOV"]])
    conditions:       dict        = Field(..., description="Rule conditions JSON")
    join_keys:        list[str]   = Field(default=["SUBJID"])
    severity:         str         = Field(default="medium")
    message_template: str
    status:           str         = Field(default="active")
    created_by:       Optional[str] = None


class RuleUpdate(BaseModel):
    name:             Optional[str]       = None
    description:      Optional[str]       = None
    rule_type:        Optional[str]       = None
    datasets:         Optional[list[str]] = None
    conditions:       Optional[dict]      = None
    join_keys:        Optional[list[str]] = None
    severity:         Optional[str]       = None
    message_template: Optional[str]       = None


class RuleStatusUpdate(BaseModel):
    status: str = Field(..., examples=["active"])


# ── Response ──────────────────────────────────────────────

class RuleResponse(BaseModel):
    id:               int
    rule_code:        str
    name:             str
    description:      Optional[str]
    rule_type:        str
    datasets:         list[str]
    conditions:       dict
    join_keys:        list[str]
    severity:         str
    message_template: str
    status:           str
    created_by:       Optional[str]
    created_at:       datetime
    updated_at:       datetime
    version:          int

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_rule(cls, rule) -> "RuleResponse":
        import json
        return cls(
            id=rule.id,
            rule_code=rule.rule_code,
            name=rule.name,
            description=rule.description,
            rule_type=rule.rule_type,
            datasets=json.loads(rule.datasets or "[]"),
            conditions=json.loads(rule.conditions or "{}"),
            join_keys=json.loads(rule.join_keys or '["SUBJID"]'),
            severity=rule.severity,
            message_template=rule.message_template,
            status=rule.status,
            created_by=rule.created_by,
            created_at=rule.created_at,
            updated_at=rule.updated_at,
            version=rule.version,
        )


# ── Test Rule ─────────────────────────────────────────────

class RuleTestResponse(BaseModel):
    matched_records: int
    flagged_issues:  int
    flag_rate:       float
    preview:         list[dict]
