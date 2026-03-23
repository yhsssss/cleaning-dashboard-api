# schemas/issue.py
from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class IssueResponse(BaseModel):
    id:            int
    run_id:        int
    rule_id:       int
    rule_code:     Optional[str] = None   # join해서 채움
    subjid:        str
    siteid:        Optional[str]
    visit:         Optional[str]
    left_dataset:  Optional[str]
    left_field:    Optional[str]
    left_value:    Optional[str]
    right_dataset: Optional[str]
    right_field:   Optional[str]
    right_value:   Optional[str]
    message:       str
    severity:      str
    issue_status:  str
    comment:       Optional[str]
    resolved_by:   Optional[str]
    resolved_at:   Optional[datetime]
    flagged_at:    datetime

    model_config = {"from_attributes": True}


class IssueStatusUpdate(BaseModel):
    issue_status: str   # open | resolved | acknowledged | waived
    comment:      Optional[str] = None
    resolved_by:  Optional[str] = None
