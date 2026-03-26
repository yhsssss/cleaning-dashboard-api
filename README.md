# Clinical Data Cleaning Dashboard — Backend

FastAPI-based backend for the Clinical Data Cleaning Assistant Tool. Enables data managers to define, manage, and execute data validation rules across multiple CRF datasets (ECG, DOV, DM, ICF, PREG, etc.).

---

## Tech Stack

- **Python 3.12**
- **FastAPI** — REST API framework
- **SQLAlchemy** — ORM
- **PostgreSQL** — Primary database (SQLite supported for local dev)
- **pandas** — CSV parsing and rule execution
- **Pydantic** — Request/response schema validation

---

## Project Structure

```
cleaning_dashboard_api/
├── main.py              # FastAPI app entry point
├── database.py          # DB connection and session management
├── models.py            # SQLAlchemy ORM models
├── rule_engine.py       # Core validation rule execution engine
├── requirements.txt
├── routers/
│   ├── rules.py         # Rule CRUD + test endpoints
│   ├── runs.py          # CSV upload + validation run endpoints
│   └── issues.py        # Issue status management endpoints
└── schemas/
    ├── rule.py          # Pydantic schemas for rules
    ├── run.py           # Pydantic schemas for runs
    └── issue.py         # Pydantic schemas for issues
```

---

## Database Schema

| Table | Description |
|---|---|
| `rules` | Validation rule definitions with JSON-based conditions |
| `runs` | CSV upload and rule execution records |
| `issues` | Individual validation issues flagged per run |
| `rule_audit_log` | Full change history for rules (GCP compliance) |
| `crf_uploads` | Metadata for uploaded CSV files |

---

## Supported Rule Types

| Type | Description |
|---|---|
| `COMPARE` | Compare two field values across datasets |
| `REQUIRED` | Field must be present when condition is met |
| `PROHIBITED` | Field must be absent when condition is met |
| `DATE_ORDER` | Date A must precede Date B |
| `DATE_WINDOW` | Date difference must be within allowed range |
| `TIME_WINDOW` | Actual time within ±N minutes of scheduled time |
| `CODELIST` | Value must be within an allowed list |
| `RANGE` | Numeric value must be within min/max bounds |
| `VISIT_COMPLETE` | Required CRF must exist for a given visit |
| `CROSS_CRF` | Same field must match across two CRFs |

---

## Getting Started

### 1. Prerequisites

- Python 3.12+
- PostgreSQL 16+

### 2. Clone and install dependencies

```bash
git clone https://github.com/your-org/cleaning_dashboard_api.git
cd cleaning_dashboard_api

python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

### 3. Set up PostgreSQL

```bash
psql postgres
```

```sql
CREATE USER clinical_user WITH PASSWORD 'clinical_pass';
CREATE DATABASE clinical_db OWNER clinical_user;
GRANT ALL PRIVILEGES ON DATABASE clinical_db TO clinical_user;
\q
```

### 4. Configure database URL

In `database.py`, update the connection string:

```python
DB_URL = "postgresql+psycopg2://clinical_user:clinical_pass@localhost:5432/clinical_db"
```

### 5. Run the server

```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`.  
Interactive API docs (Swagger UI): `http://localhost:8000/docs`

---

## API Endpoints

### Rules

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/rules` | List all rules (filter by status, rule_type) |
| `POST` | `/api/rules` | Create a new rule |
| `GET` | `/api/rules/{id}` | Get rule detail |
| `PUT` | `/api/rules/{id}` | Update a rule |
| `DELETE` | `/api/rules/{id}` | Delete a rule |
| `PATCH` | `/api/rules/{id}/status` | Toggle active/inactive |
| `POST` | `/api/rules/{id}/test` | Test rule against sample CSV files |

### Runs

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/runs` | Upload CSV files and execute validation |
| `GET` | `/api/runs` | List run history |
| `GET` | `/api/runs/{id}` | Get run detail |
| `GET` | `/api/runs/{id}/issues` | List issues for a run (filterable) |
| `GET` | `/api/runs/{id}/summary` | KPI + chart data for dashboard |

### Issues

| Method | Endpoint | Description |
|---|---|---|
| `PATCH` | `/api/issues/{id}/status` | Update issue status (open / resolved / acknowledged / waived) |

---

## Rule Conditions JSON Structure

Rules store their logic as structured JSON in the `conditions` column. This allows the Rule Engine to execute them programmatically without any hardcoded logic per rule.

**Example — COMPARE rule:**

```json
{
  "logic": "AND",
  "filters": [
    { "dataset": "ECG", "field": "VISIT", "op": "IN", "value": ["Day 1", "FE Day 1"] },
    { "dataset": "ECG", "field": "ECGYN", "op": "=", "value": "Yes" }
  ],
  "compare": {
    "left":  { "dataset": "ECG", "field": "ECGD1DAT" },
    "op":    "=",
    "right": { "dataset": "DOV", "field": "DOVDAT" }
  }
}
```

**Example — DATE_WINDOW rule:**

```json
{
  "logic": "AND",
  "filters": [],
  "date_window": {
    "anchor":     { "dataset": "DOV", "field": "SCRDAT" },
    "target":     { "dataset": "ICF", "field": "ICFDAT" },
    "direction":  "before",
    "max_days":   30,
    "allow_same": true
  }
}
```

---

## Running Validation

Send a `POST /api/runs` request with CSV files as `multipart/form-data`:

```bash
curl -X POST http://localhost:8000/api/runs \
  -F "files=@ECG.csv" \
  -F "files=@DOV.csv" \
  -F "files=@DM.csv" \
  -F 'crf_names=["ECG","DOV","DM"]' \
  -F "study_id=TRIAL-2025-A" \
  -F "rule_ids=null" \
  -F "created_by=admin"
```

The response includes a run summary with total issues, subjects/sites impacted, and severity breakdown.

---

## Health Check

```bash
curl http://localhost:8000/health
# {"status": "ok"}
```

---

## Notes

- All CSV columns are read as strings to prevent type coercion errors during rule execution.
- The `rule_audit_log` table tracks all rule changes for GCP / 21 CFR Part 11 compliance.
- PostgreSQL is recommended for production. SQLite can be used for local development by changing `DB_URL` in `database.py`.
