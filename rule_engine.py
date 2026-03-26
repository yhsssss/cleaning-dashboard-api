# rule_engine.py
# Clinical Data Cleaning Assistant — Rule Engine
# ============================================================
# pip install pandas sqlalchemy
#
# 구조:
#   RuleEngine          — 메인 진입점. run 생성 + 규칙 실행 총괄
#   BaseRuleExecutor    — 각 rule type executor의 추상 베이스
#   CompareExecutor     — COMPARE
#   RequiredExecutor    — REQUIRED / PROHIBITED
#   DateOrderExecutor   — DATE_ORDER
#   DateWindowExecutor  — DATE_WINDOW
#   TimeWindowExecutor  — TIME_WINDOW
#   CodelistExecutor    — CODELIST
#   RangeExecutor       — RANGE
#   VisitCompleteExecutor — VISIT_COMPLETE
#   CrossCRFExecutor    — CROSS_CRF
# ============================================================

import json
import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from models import Rule, Run, Issue, CRFUpload

logger = logging.getLogger(__name__)


# ============================================================
# 예외
# ============================================================
class RuleEngineError(Exception):
    pass

class MissingCRFError(RuleEngineError):
    pass

class InvalidConditionsError(RuleEngineError):
    pass


# ============================================================
# 헬퍼
# ============================================================
def _load_json(value: Optional[str]) -> any:
    if not value:
        return None
    return json.loads(value)


def _file_hash(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _format_message(template: str, row: dict) -> str:
    """메시지 템플릿의 {SUBJID}, {VISIT} 등을 실제 값으로 치환."""
    try:
        return template.format(**{k: v for k, v in row.items() if isinstance(k, str)})
    except KeyError:
        return template


# ============================================================
# 필터 적용 (conditions.filters → DataFrame 필터링)
# ============================================================
def _apply_filters(df: pd.DataFrame, filters: list, logic: str = "AND") -> pd.DataFrame:
    """
    filters: [
        {"dataset": "ECG", "field": "VISIT", "op": "IN", "value": ["Day 1", "FE Day 1"]},
        {"dataset": "ECG", "field": "ECGYN", "op": "=",  "value": "Yes"},
    ]
    merge 후 df 컬럼명은 "{DATASET}_{FIELD}" 또는 "{FIELD}" 형태.
    """
    if not filters:
        return df

    masks = []
    for f in filters:
        field = f["field"]
        op    = f["op"]
        value = f["value"]

        col = (
            field                          if field                          in df.columns else
            f"{field}_{f['dataset']}"      if f"{field}_{f['dataset']}"      in df.columns else
            f"{f['dataset']}_{field}"      if f"{f['dataset']}_{field}"      in df.columns else
            field  # fallback
        )
        
        if col not in df.columns:
            logger.warning(f"Filter field '{col}' not found in DataFrame. Skipping.")
            continue

        if op == "=":
            masks.append(df[col] == value)
        elif op == "!=":
            masks.append(df[col] != value)
        elif op == "IN":
            masks.append(df[col].isin(value if isinstance(value, list) else [value]))
        elif op == "NOT IN":
            masks.append(~df[col].isin(value if isinstance(value, list) else [value]))
        elif op == ">":
            masks.append(df[col] > value)
        elif op == ">=":
            masks.append(df[col] >= value)
        elif op == "<":
            masks.append(df[col] < value)
        elif op == "<=":
            masks.append(df[col] <= value)
        else:
            logger.warning(f"Unknown operator '{op}'. Skipping filter.")

    if not masks:
        return df

    combined = masks[0]
    for m in masks[1:]:
        combined = combined & m if logic == "AND" else combined | m

    return df[combined]


# ============================================================
# Base Executor
# ============================================================
class BaseRuleExecutor(ABC):
    """모든 rule type executor의 추상 베이스."""

    def __init__(self, rule: Rule, crf_data: dict[str, pd.DataFrame]):
        """
        rule     : Rule ORM 객체
        crf_data : {"ECG": pd.DataFrame, "DOV": pd.DataFrame, ...}
        """
        self.rule      = rule
        self.crf_data  = crf_data
        self.cond      = _load_json(rule.conditions) or {}
        self.join_keys = _load_json(rule.join_keys)  or ["SUBJID"]
        self.filters   = self.cond.get("filters", [])
        self.logic     = self.cond.get("logic", "AND")

    def _get_crf(self, name: str) -> pd.DataFrame:
        if name not in self.crf_data:
            raise MissingCRFError(f"CRF '{name}' not found. Available: {list(self.crf_data.keys())}")
        return self.crf_data[name].copy()

    def _merge(self, left_ds: str, right_ds: str) -> pd.DataFrame:
        """두 CRF를 join_keys로 merge. 컬럼 충돌 시 suffixes로 구분."""
        left  = self._get_crf(left_ds)
        right = self._get_crf(right_ds)

        # 공통 join_keys만 사용
        keys = [k for k in self.join_keys if k in left.columns and k in right.columns]
        if not keys:
            raise RuleEngineError(
                f"No valid join keys {self.join_keys} between {left_ds} and {right_ds}."
            )

        merged = left.merge(
            right,
            on=keys,
            how="inner",
            suffixes=(f"_{left_ds}", f"_{right_ds}"),
        )
        return merged

    def _build_issue(self, row: pd.Series,
                     left_field: str = None, left_value=None,
                     right_field: str = None, right_value=None) -> dict:
        return {
            "rule_id":       self.rule.id,
            "subjid":        str(row.get("SUBJID", "")),
            "siteid":        str(row.get("SITEID", "")) if "SITEID" in row else None,
            "visit":         str(row.get("VISIT",  "")) if "VISIT"  in row else None,
            "left_dataset":  self.cond.get("compare", {}).get("left",  {}).get("dataset"),
            "left_field":    left_field,
            "left_value":    str(left_value)  if left_value  is not None else None,
            "right_dataset": self.cond.get("compare", {}).get("right", {}).get("dataset"),
            "right_field":   right_field,
            "right_value":   str(right_value) if right_value is not None else None,
            "message":       _format_message(self.rule.message_template, row.to_dict()),
            "severity":      self.rule.severity,
        }

    @abstractmethod
    def execute(self) -> list[dict]:
        """이슈 dict 목록 반환. 각 dict는 _build_issue() 구조."""
        ...


# ============================================================
# COMPARE
# ============================================================
class CompareExecutor(BaseRuleExecutor):
    """
    conditions.compare:
        left:  {dataset, field}
        op:    = | != | < | <= | > | >=
        right: {dataset, field}  or  {value: "literal"}
    """

    def execute(self) -> list[dict]:
        compare = self.cond.get("compare")
        if not compare:
            raise InvalidConditionsError("COMPARE rule missing 'compare' block.")

        left_ds    = compare["left"]["dataset"]
        left_field = compare["left"]["field"]
        op         = compare["op"]

        # right 가 다른 dataset 필드인지, 리터럴 값인지 구분
        right_is_field = "dataset" in compare["right"]
        if right_is_field:
            right_ds    = compare["right"]["dataset"]
            right_field = compare["right"]["field"]
            df = self._merge(left_ds, right_ds)
            l_col = f"{left_field}_{left_ds}"   if f"{left_field}_{left_ds}"   in df.columns else left_field
            r_col = f"{right_field}_{right_ds}" if f"{right_field}_{right_ds}" in df.columns else right_field
        else:
            df = self._get_crf(left_ds)
            l_col = left_field
            r_col = None

        df = _apply_filters(df, self.filters, self.logic)

        right_literal = None if right_is_field else compare["right"].get("value")

        def _flag(row):
            l = row[l_col]
            r = row[r_col] if right_is_field else right_literal
            if op == "=":   return l != r
            if op == "!=":  return l == r
            if op == "<":   return not (l <  r)
            if op == "<=":  return not (l <= r)
            if op == ">":   return not (l >  r)
            if op == ">=":  return not (l >= r)
            return False

        issues = []
        for _, row in df.iterrows():
            if _flag(row):
                issues.append(self._build_issue(
                    row,
                    left_field=left_field,  left_value=row[l_col],
                    right_field=right_field if right_is_field else None,
                    right_value=row[r_col]  if right_is_field else right_literal,
                ))
        return issues


# ============================================================
# REQUIRED / PROHIBITED
# ============================================================
class RequiredExecutor(BaseRuleExecutor):
    """
    conditions.target:
        dataset : str
        field   : str
        expect  : "not_null" (REQUIRED) | "null" (PROHIBITED)
    """

    def execute(self) -> list[dict]:
        target = self.cond.get("target")
        if not target:
            raise InvalidConditionsError("REQUIRED rule missing 'target' block.")

        ds     = target["dataset"]
        field  = target["field"]
        expect = target.get("expect", "not_null")

        # 조건 CRF (filters) 와 target CRF 가 다를 수 있음 → merge 필요 시
        filter_datasets = {f["dataset"] for f in self.filters}
        filter_datasets.discard(ds)

        if filter_datasets:
            # 조건이 다른 CRF에 있는 경우 merge
            other_ds = filter_datasets.pop()
            df = self._merge(ds, other_ds)
        else:
            df = self._get_crf(ds)

        df = _apply_filters(df, self.filters, self.logic)

        col = field if field in df.columns else f"{field}_{ds}"
        if col not in df.columns:
            raise InvalidConditionsError(f"Field '{field}' not found in '{ds}'.")

        if expect == "not_null":
            flagged = df[df[col].isna() | (df[col].astype(str).str.strip() == "")]
        else:  # null / prohibited
            flagged = df[df[col].notna() & (df[col].astype(str).str.strip() != "")]

        issues = []
        for _, row in flagged.iterrows():
            issues.append(self._build_issue(
                row, left_field=field, left_value=row.get(col)
            ))
        return issues


# ============================================================
# DATE_ORDER
# ============================================================
class DateOrderExecutor(BaseRuleExecutor):
    """
    conditions.date_order:
        earlier: {dataset, field}   — 더 이른 날짜여야 하는 쪽
        later:   {dataset, field}   — 더 늦은 날짜여야 하는 쪽
        allow_same: bool (default true)
    """

    def execute(self) -> list[dict]:
        do = self.cond.get("date_order")
        if not do:
            raise InvalidConditionsError("DATE_ORDER rule missing 'date_order' block.")

        early_ds    = do["earlier"]["dataset"]
        early_field = do["earlier"]["field"]
        late_ds     = do["later"]["dataset"]
        late_field  = do["later"]["field"]
        allow_same  = do.get("allow_same", True)

        df = self._merge(early_ds, late_ds)
        df = _apply_filters(df, self.filters, self.logic)

        e_col = f"{early_field}_{early_ds}" if f"{early_field}_{early_ds}" in df.columns else early_field
        l_col = f"{late_field}_{late_ds}"   if f"{late_field}_{late_ds}"   in df.columns else late_field

        df[e_col] = pd.to_datetime(df[e_col], errors="coerce")
        df[l_col] = pd.to_datetime(df[l_col], errors="coerce")

        if allow_same:
            flagged = df[df[e_col] > df[l_col]]
        else:
            flagged = df[df[e_col] >= df[l_col]]

        issues = []
        for _, row in flagged.iterrows():
            issues.append(self._build_issue(
                row,
                left_field=early_field, left_value=row[e_col],
                right_field=late_field, right_value=row[l_col],
            ))
        return issues


# ============================================================
# DATE_WINDOW
# ============================================================
class DateWindowExecutor(BaseRuleExecutor):
    """
    conditions.date_window:
        anchor:     {dataset, field}   — 기준 날짜
        target:     {dataset, field}   — 비교할 날짜
        direction:  "before" | "after" | "either"
        max_days:   int
        allow_same: bool (default true)
    """

    def execute(self) -> list[dict]:
        dw = self.cond.get("date_window")
        if not dw:
            raise InvalidConditionsError("DATE_WINDOW rule missing 'date_window' block.")

        anchor_ds    = dw["anchor"]["dataset"]
        anchor_field = dw["anchor"]["field"]
        target_ds    = dw["target"]["dataset"]
        target_field = dw["target"]["field"]
        direction    = dw.get("direction", "either")
        max_days     = int(dw.get("max_days", 0))
        allow_same   = dw.get("allow_same", True)

        df = self._merge(anchor_ds, target_ds)
        df = _apply_filters(df, self.filters, self.logic)

        a_col = f"{anchor_field}_{anchor_ds}" if f"{anchor_field}_{anchor_ds}" in df.columns else anchor_field
        t_col = f"{target_field}_{target_ds}" if f"{target_field}_{target_ds}" in df.columns else target_field

        df[a_col] = pd.to_datetime(df[a_col], errors="coerce")
        df[t_col] = pd.to_datetime(df[t_col], errors="coerce")

        df["_diff"] = (df[t_col] - df[a_col]).dt.days  # target - anchor

        if direction == "before":
            # target 이 anchor 이전이어야 함 → diff <= 0
            if allow_same:
                flagged = df[(df["_diff"] > 0) | (df["_diff"] < -max_days)]
            else:
                flagged = df[(df["_diff"] >= 0) | (df["_diff"] < -max_days)]
        elif direction == "after":
            # target 이 anchor 이후여야 함 → diff >= 0
            if allow_same:
                flagged = df[(df["_diff"] < 0) | (df["_diff"] > max_days)]
            else:
                flagged = df[(df["_diff"] <= 0) | (df["_diff"] > max_days)]
        else:  # either
            flagged = df[df["_diff"].abs() > max_days]

        issues = []
        for _, row in flagged.iterrows():
            issues.append(self._build_issue(
                row,
                left_field=anchor_field, left_value=row[a_col],
                right_field=target_field, right_value=row[t_col],
            ))
        return issues


# ============================================================
# TIME_WINDOW
# ============================================================
class TimeWindowExecutor(BaseRuleExecutor):
    """
    conditions.time_window:
        actual:    {dataset, field}   — 실제 시간 (HH:MM 문자열)
        scheduled: {dataset, field}   — 예정 시간 (HH:MM 문자열)
        max_minutes: int
    """

    def execute(self) -> list[dict]:
        tw = self.cond.get("time_window")
        if not tw:
            raise InvalidConditionsError("TIME_WINDOW rule missing 'time_window' block.")

        actual_ds      = tw["actual"]["dataset"]
        actual_field   = tw["actual"]["field"]
        sched_ds       = tw["scheduled"]["dataset"]
        sched_field    = tw["scheduled"]["field"]
        max_minutes    = int(tw.get("max_minutes", 15))

        df = self._merge(actual_ds, sched_ds)
        df = _apply_filters(df, self.filters, self.logic)

        a_col = f"{actual_field}_{actual_ds}"   if f"{actual_field}_{actual_ds}"   in df.columns else actual_field
        s_col = f"{sched_field}_{sched_ds}"     if f"{sched_field}_{sched_ds}"     in df.columns else sched_field

        def _to_minutes(t) -> Optional[int]:
            try:
                h, m = str(t).strip().split(":")
                return int(h) * 60 + int(m)
            except Exception:
                return None

        df["_actual_min"] = df[a_col].apply(_to_minutes)
        df["_sched_min"]  = df[s_col].apply(_to_minutes)
        df["_diff_min"]   = (df["_actual_min"] - df["_sched_min"]).abs()

        flagged = df[df["_diff_min"] > max_minutes]

        issues = []
        for _, row in flagged.iterrows():
            issues.append(self._build_issue(
                row,
                left_field=actual_field,  left_value=row[a_col],
                right_field=sched_field,  right_value=row[s_col],
            ))
        return issues


# ============================================================
# CODELIST
# ============================================================
class CodelistExecutor(BaseRuleExecutor):
    """
    conditions.codelist:
        dataset  : str
        field    : str
        allowed  : [str, ...]   — 허용 값 목록
    """

    def execute(self) -> list[dict]:
        cl = self.cond.get("codelist")
        if not cl:
            raise InvalidConditionsError("CODELIST rule missing 'codelist' block.")

        ds      = cl["dataset"]
        field   = cl["field"]
        allowed = cl.get("allowed", [])

        df  = self._get_crf(ds)
        df  = _apply_filters(df, self.filters, self.logic)
        col = field if field in df.columns else f"{field}_{ds}"

        flagged = df[~df[col].isin(allowed)]

        issues = []
        for _, row in flagged.iterrows():
            issues.append(self._build_issue(
                row, left_field=field, left_value=row.get(col)
            ))
        return issues


# ============================================================
# RANGE
# ============================================================
class RangeExecutor(BaseRuleExecutor):
    """
    conditions.range:
        dataset : str
        field   : str
        min     : number (inclusive)
        max     : number (inclusive)
    """

    def execute(self) -> list[dict]:
        rng = self.cond.get("range")
        if not rng:
            raise InvalidConditionsError("RANGE rule missing 'range' block.")

        ds    = rng["dataset"]
        field = rng["field"]
        vmin  = rng.get("min")
        vmax  = rng.get("max")

        df  = self._get_crf(ds)
        df  = _apply_filters(df, self.filters, self.logic)
        col = field if field in df.columns else f"{field}_{ds}"

        df[col] = pd.to_numeric(df[col], errors="coerce")

        mask = pd.Series([False] * len(df), index=df.index)
        if vmin is not None:
            mask = mask | (df[col] < vmin)
        if vmax is not None:
            mask = mask | (df[col] > vmax)
        mask = mask | df[col].isna()

        flagged = df[mask]

        issues = []
        for _, row in flagged.iterrows():
            issues.append(self._build_issue(
                row, left_field=field, left_value=row.get(col)
            ))
        return issues


# ============================================================
# VISIT_COMPLETE
# ============================================================
class VisitCompleteExecutor(BaseRuleExecutor):
    """
    특정 visit에서 특정 CRF 레코드가 존재해야 함.

    conditions.visit_complete:
        anchor_dataset  : str    — 방문 기준 CRF (e.g. "DOV")
        anchor_visit_field: str  — 방문 컬럼명 (e.g. "VISIT")
        required_visits : [str]  — 체크할 방문 목록
        target_dataset  : str    — 존재해야 하는 CRF (e.g. "ECG")
    """

    def execute(self) -> list[dict]:
        vc = self.cond.get("visit_complete")
        if not vc:
            raise InvalidConditionsError("VISIT_COMPLETE rule missing 'visit_complete' block.")

        anchor_ds      = vc["anchor_dataset"]
        visit_field    = vc.get("anchor_visit_field", "VISIT")
        req_visits     = vc.get("required_visits", [])
        target_ds      = vc["target_dataset"]

        anchor_df = self._get_crf(anchor_ds)
        target_df = self._get_crf(target_ds)

        # 해당 visit 필터링
        anchor_df = anchor_df[anchor_df[visit_field].isin(req_visits)]

        # target에 없는 subject+visit 찾기
        keys = [k for k in self.join_keys if k in anchor_df.columns and k in target_df.columns]
        merged = anchor_df.merge(
            target_df[keys].drop_duplicates(),
            on=keys, how="left", indicator=True
        )
        flagged = merged[merged["_merge"] == "left_only"]

        issues = []
        for _, row in flagged.iterrows():
            issues.append(self._build_issue(
                row,
                left_field=visit_field, left_value=row.get(visit_field),
            ))
        return issues


# ============================================================
# CROSS_CRF
# ============================================================
class CrossCRFExecutor(BaseRuleExecutor):
    """
    두 CRF 간 동일해야 하는 필드 일치 여부 확인.
    (CompareExecutor와 유사하나 join 전략이 subject 레벨)

    conditions.cross_crf:
        left:  {dataset, field}
        right: {dataset, field}
    """

    def execute(self) -> list[dict]:
        cc = self.cond.get("cross_crf")
        if not cc:
            raise InvalidConditionsError("CROSS_CRF rule missing 'cross_crf' block.")

        left_ds    = cc["left"]["dataset"]
        left_field = cc["left"]["field"]
        right_ds   = cc["right"]["dataset"]
        right_field = cc["right"]["field"]

        df = self._merge(left_ds, right_ds)
        df = _apply_filters(df, self.filters, self.logic)

        l_col = f"{left_field}_{left_ds}"   if f"{left_field}_{left_ds}"   in df.columns else left_field
        r_col = f"{right_field}_{right_ds}" if f"{right_field}_{right_ds}" in df.columns else right_field

        flagged = df[df[l_col] != df[r_col]]

        issues = []
        for _, row in flagged.iterrows():
            issues.append(self._build_issue(
                row,
                left_field=left_field,   left_value=row[l_col],
                right_field=right_field, right_value=row[r_col],
            ))
        return issues


# ============================================================
# Executor 레지스트리
# ============================================================
EXECUTOR_MAP: dict[str, type[BaseRuleExecutor]] = {
    "COMPARE":        CompareExecutor,
    "REQUIRED":       RequiredExecutor,
    "PROHIBITED":     RequiredExecutor,      # 동일 executor, expect="null"로 처리
    "DATE_ORDER":     DateOrderExecutor,
    "DATE_WINDOW":    DateWindowExecutor,
    "TIME_WINDOW":    TimeWindowExecutor,
    "CODELIST":       CodelistExecutor,
    "RANGE":          RangeExecutor,
    "VISIT_COMPLETE": VisitCompleteExecutor,
    "CROSS_CRF":      CrossCRFExecutor,
}


# ============================================================
# RuleEngine — 메인 진입점
# ============================================================
class RuleEngine:
    """
    사용 예:
        engine = RuleEngine(db_session)
        run = engine.execute_run(
            study_id="TRIAL-2025-A",
            crf_files={"ECG": "ECG.csv", "DOV": "DOV.csv", "DM": "DM.csv"},
            rule_ids=None,   # None = 전체 active 규칙 적용
            created_by="admin",
        )
        print(run.total_issues)
    """

    def __init__(self, db: Session):
        self.db = db

    # ── CSV 로드 ──────────────────────────────────────────

    def _load_crfs(self, crf_files: dict[str, str]) -> dict[str, pd.DataFrame]:
        """crf_files: {"ECG": "/path/ECG.csv", ...} → DataFrame dict"""
        crfs = {}
        for crf_name, filepath in crf_files.items():
            try:
                df = pd.read_csv(filepath, dtype=str)   # 모두 str로 읽어 타입 오류 방지
                df.columns = [c.strip().upper() for c in df.columns]
                crfs[crf_name.upper()] = df
                logger.info(f"Loaded {crf_name}: {len(df)} rows, {len(df.columns)} cols")
            except Exception as e:
                raise RuleEngineError(f"Failed to load CRF '{crf_name}' from '{filepath}': {e}")
        return crfs

    # ── Run 생성 ──────────────────────────────────────────

    def _create_run(self, study_id: str, crf_files: dict[str, str],
                    crfs: dict[str, pd.DataFrame],
                    rule_ids: Optional[list[int]], created_by: str) -> Run:
        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        run_code = f"RUN-{ts}"

        uploaded = []
        for name, path in crf_files.items():
            df = crfs.get(name.upper(), pd.DataFrame())
            uploaded.append({
                "crf": name.upper(),
                "filename": path.split("/")[-1],
                "rows": len(df),
                "hash": _file_hash(path),
            })

        run = Run(
            run_code=run_code,
            study_id=study_id,
            status="running",
            started_at=datetime.utcnow(),
            created_by=created_by,
        )
        run.uploaded_files_list  = uploaded
        run.applied_rule_ids_list = rule_ids
        self.db.add(run)
        self.db.flush()   # id 확보

        # CRFUpload 메타 저장
        for name, path in crf_files.items():
            df = crfs.get(name.upper(), pd.DataFrame())
            upload = CRFUpload(
                run_id=run.id,
                crf_name=name.upper(),
                original_filename=path.split("/")[-1],
                row_count=len(df),
                file_hash=_file_hash(path),
            )
            upload.column_names = json.dumps(list(df.columns))
            self.db.add(upload)

        return run

    # ── 규칙 조회 ─────────────────────────────────────────

    def _get_rules(self, rule_ids: Optional[list[int]]) -> list[Rule]:
        q = self.db.query(Rule).filter(Rule.status == "active")
        if rule_ids:
            q = q.filter(Rule.id.in_(rule_ids))
        rules = q.all()
        if not rules:
            logger.warning("No active rules found.")
        return rules

    # ── 단일 규칙 실행 ────────────────────────────────────

    def _execute_rule(self, rule: Rule,
                      crfs: dict[str, pd.DataFrame]) -> list[dict]:
        executor_cls = EXECUTOR_MAP.get(rule.rule_type.upper())
        if not executor_cls:
            logger.warning(f"No executor for rule_type='{rule.rule_type}'. Skipping.")
            return []

        try:
            executor = executor_cls(rule, crfs)
            issues   = executor.execute()
            logger.info(f"  {rule.rule_code} [{rule.rule_type}] → {len(issues)} issues")
            return issues
        except MissingCRFError as e:
            logger.warning(f"  {rule.rule_code} skipped: {e}")
            return []
        except Exception as e:
            logger.error(f"  {rule.rule_code} failed: {e}", exc_info=True)
            return []

    # ── 이슈 저장 ─────────────────────────────────────────

    def _save_issues(self, run_id: int, issue_dicts: list[dict]) -> list[Issue]:
        issues = []
        for d in issue_dicts:
            issue = Issue(run_id=run_id, **d)
            self.db.add(issue)
            issues.append(issue)
        return issues

    # ── Run 통계 업데이트 ─────────────────────────────────

    def _update_run_stats(self, run: Run, issues: list[Issue]):
        run.total_issues      = len(issues)
        run.high_issues       = sum(1 for i in issues if i.severity == "high")
        run.medium_issues     = sum(1 for i in issues if i.severity == "medium")
        run.low_issues        = sum(1 for i in issues if i.severity == "low")
        run.subjects_impacted = len({i.subjid for i in issues})
        run.sites_impacted    = len({i.siteid for i in issues if i.siteid})
        run.status            = "done"
        run.finished_at       = datetime.utcnow()

    # ── 메인 실행 ─────────────────────────────────────────

    def execute_run(
        self,
        study_id: str,
        crf_files: dict[str, str],   # {"ECG": "/path/ECG.csv", ...}
        rule_ids: Optional[list[int]] = None,
        created_by: str = "system",
    ) -> Run:
        """
        전체 실행 흐름:
        1. CSV 로드
        2. Run 생성
        3. 규칙 목록 조회
        4. 규칙별 executor 실행
        5. 이슈 저장
        6. Run 통계 업데이트
        7. commit
        """
        logger.info(f"=== RuleEngine.execute_run | study={study_id} ===")

        # 1. CSV 로드
        crfs = self._load_crfs(crf_files)

        # 2. Run 생성
        run = self._create_run(study_id, crf_files, crfs, rule_ids, created_by)

        try:
            # 3. 규칙 조회
            rules = self._get_rules(rule_ids)
            logger.info(f"Applying {len(rules)} rules...")

            # 4. 규칙별 실행
            all_issue_dicts = []
            for rule in rules:
                issue_dicts = self._execute_rule(rule, crfs)
                all_issue_dicts.extend(issue_dicts)

            # 5. 이슈 저장
            saved_issues = self._save_issues(run.id, all_issue_dicts)

            # 6. Run 통계 업데이트
            self._update_run_stats(run, saved_issues)

            # 7. commit
            self.db.commit()
            logger.info(f"=== Done: {run.run_code} | {run.total_issues} issues ===")

        except Exception as e:
            self.db.rollback()
            run.status        = "failed"
            run.error_message = str(e)
            run.finished_at   = datetime.utcnow()
            self.db.commit()
            logger.error(f"Run failed: {e}", exc_info=True)
            raise

        return run

    # ── 단일 규칙 테스트 (저장 없이 미리보기) ────────────

    def test_rule(
        self,
        rule: Rule,
        crf_files: dict[str, str],
    ) -> dict:
        """
        Rule Builder의 "Test Rule" 기능.
        이슈를 DB에 저장하지 않고 결과만 반환.

        Returns:
            {
                "matched_records": int,
                "flagged_issues":  int,
                "flag_rate":       float,
                "preview":         list[dict]   # 최대 20건
            }
        """
        crfs         = self._load_crfs(crf_files)
        issue_dicts  = self._execute_rule(rule, crfs)

        # matched_records: 조건 필터 적용 후 행 수 계산
        executor_cls = EXECUTOR_MAP.get(rule.rule_type.upper())
        matched = 0
        if executor_cls:
            try:
                ex = executor_cls(rule, crfs)
                ds_name = _load_json(rule.datasets)[0] if _load_json(rule.datasets) else None
                if ds_name and ds_name in crfs:
                    matched = len(crfs[ds_name])
            except Exception:
                matched = len(issue_dicts)

        flagged   = len(issue_dicts)
        flag_rate = round(flagged / matched * 100, 1) if matched > 0 else 0.0

        return {
            "matched_records": matched,
            "flagged_issues":  flagged,
            "flag_rate":       flag_rate,
            "preview":         issue_dicts[:20],
        }


# ============================================================
# 사용 예시
# ============================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    from models import init_db
    from sqlalchemy.orm import Session

    engine = init_db("sqlite:///./clinical.db")

    with Session(engine) as session:
        re = RuleEngine(session)

        run = re.execute_run(
            study_id="TRIAL-2025-A",
            crf_files={
                "ECG": "sample_data/ECG.csv",
                "DOV": "sample_data/DOV.csv",
                "DM":  "sample_data/DM.csv",
            },
            rule_ids=None,       # None = 전체 active 규칙
            created_by="admin",
        )

        print(f"\nRun:      {run.run_code}")
        print(f"Status:   {run.status}")
        print(f"Issues:   {run.total_issues} "
              f"(H:{run.high_issues} M:{run.medium_issues} L:{run.low_issues})")
        print(f"Subjects: {run.subjects_impacted}")
        print(f"Sites:    {run.sites_impacted}")
