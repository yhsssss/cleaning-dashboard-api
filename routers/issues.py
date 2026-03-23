# routers/issues.py
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import get_db
from models import Issue
from schemas.issue import IssueStatusUpdate

router = APIRouter(prefix="/api/issues", tags=["Issues"])

VALID_STATUSES = {"open", "resolved", "acknowledged", "waived"}


@router.patch("/{issue_id}/status")
def update_issue_status(
    issue_id: int,
    body: IssueStatusUpdate,
    db: Session = Depends(get_db),
):
    """DM이 이슈 상태를 업데이트 (open → resolved / acknowledged / waived)."""
    issue = db.query(Issue).filter(Issue.id == issue_id).first()
    if not issue:
        raise HTTPException(status_code=404, detail=f"Issue {issue_id} not found.")

    if body.issue_status not in VALID_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"issue_status must be one of {sorted(VALID_STATUSES)}.",
        )

    issue.issue_status = body.issue_status
    if body.comment:
        issue.comment = body.comment
    if body.resolved_by:
        issue.resolved_by  = body.resolved_by
        issue.resolved_at  = datetime.utcnow()

    db.commit()
    db.refresh(issue)

    return {
        "id":           issue.id,
        "issue_status": issue.issue_status,
        "comment":      issue.comment,
        "resolved_by":  issue.resolved_by,
        "resolved_at":  issue.resolved_at.isoformat() if issue.resolved_at else None,
    }
