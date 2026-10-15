from flask import Flask, render_template, request, jsonify
from pathlib import Path
import sqlite3
import os
import pandas as pd
import json
from datetime import datetime

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DB_PATH = Path("/tmp/pipeline.db") if os.environ.get("VERCEL") else ROOT / "pipeline.db"
CSV_PATH = DATA_DIR / "india_economy_sample.csv"

app = Flask(__name__)

def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_db():
    with connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL,
                sector TEXT NOT NULL,
                month TEXT NOT NULL,
                metric TEXT NOT NULL,
                value REAL NOT NULL,
                source TEXT NOT NULL,
                ingested_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at TEXT NOT NULL,
                source_rows INTEGER NOT NULL,
                clean_rows INTEGER NOT NULL,
                rejected_rows INTEGER NOT NULL,
                status TEXT NOT NULL,
                summary TEXT NOT NULL
            )
        """)

def create_sample_csv():
    if CSV_PATH.exists():
        return
    rows = [
        ("Madhya Pradesh","Agriculture","2026-01","Production Index",108.4,"Sample dataset"),
        ("Maharashtra","Manufacturing","2026-01","Production Index",121.7,"Sample dataset"),
        ("Karnataka","Technology","2026-01","Employment Index",134.2,"Sample dataset"),
        ("Tamil Nadu","Manufacturing","2026-01","Production Index",117.9,"Sample dataset"),
        ("Gujarat","Manufacturing","2026-01","Export Index",126.5,"Sample dataset"),
        ("Rajasthan","Agriculture","2026-02","Production Index",103.8,"Sample dataset"),
        ("Uttar Pradesh","Agriculture","2026-02","Production Index",111.2,"Sample dataset"),
        ("Telangana","Technology","2026-02","Employment Index",129.6,"Sample dataset"),
        ("Delhi","Services","2026-02","Business Activity Index",132.1,"Sample dataset"),
        ("Kerala","Services","2026-02","Business Activity Index",109.7,"Sample dataset"),
        ("Punjab","Agriculture","2026-03","Production Index",106.1,"Sample dataset"),
        ("Haryana","Manufacturing","2026-03","Production Index",114.4,"Sample dataset"),
        ("West Bengal","Services","2026-03","Business Activity Index",105.9,"Sample dataset"),
        ("Andhra Pradesh","Manufacturing","2026-03","Export Index",118.3,"Sample dataset"),
        ("Odisha","Manufacturing","2026-03","Production Index",112.6,"Sample dataset"),
        ("Bihar","Agriculture","2026-03","Production Index",99.4,"Sample dataset"),
        ("Assam","Agriculture","2026-04","Production Index",102.8,"Sample dataset"),
        ("Goa","Services","2026-04","Business Activity Index",124.1,"Sample dataset"),
        ("Chhattisgarh","Manufacturing","2026-04","Production Index",110.7,"Sample dataset"),
        ("Jharkhand","Manufacturing","2026-04","Export Index",107.6,"Sample dataset"),
        ("Himachal Pradesh","Services","2026-04","Business Activity Index",115.8,"Sample dataset"),
        ("Uttarakhand","Manufacturing","2026-05","Production Index",119.3,"Sample dataset"),
        ("Sikkim","Services","2026-05","Business Activity Index",121.4,"Sample dataset"),
        ("Tripura","Agriculture","2026-05","Production Index",101.7,"Sample dataset"),
        ("Meghalaya","Agriculture","2026-05","Production Index",104.2,"Sample dataset"),
        ("Manipur","Services","2026-06","Business Activity Index",98.6,"Sample dataset"),
        ("Mizoram","Agriculture","2026-06","Production Index",100.9,"Sample dataset"),
        ("Nagaland","Agriculture","2026-06","Production Index",102.1,"Sample dataset"),
        ("Arunachal Pradesh","Agriculture","2026-06","Production Index",103.4,"Sample dataset"),
        ("Jammu and Kashmir","Services","2026-06","Business Activity Index",111.9,"Sample dataset"),
        # Intentional quality issues demonstrate cleaning and validation.
        ("Maharashtra","Manufacturing","2026-01","Production Index",121.7,"Sample dataset"),
        ("Karnataka","Technology","2026-02","Employment Index",-4.0,"Sample dataset"),
        ("Unknown","Services","not-a-month","Business Activity Index",None,"Sample dataset"),
    ]
    pd.DataFrame(rows, columns=["state","sector","month","metric","value","source"]).to_csv(CSV_PATH, index=False)

def run_pipeline():
    create_sample_csv()
    raw = pd.read_csv(CSV_PATH)
    source_rows = len(raw)
    df = raw.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    required = ["state", "sector", "month", "metric", "value", "source"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    df["state"] = df["state"].astype("string").str.strip()
    df["sector"] = df["sector"].astype("string").str.strip()
    df["month"] = df["month"].astype("string").str.strip()
    df["metric"] = df["metric"].astype("string").str.strip()
    df["source"] = df["source"].astype("string").str.strip()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    month_ok = df["month"].str.match(r"^\d{4}-(0[1-9]|1[0-2])$", na=False)
    valid = (
        df["state"].notna() & (df["state"] != "") &
        df["sector"].notna() & (df["sector"] != "") &
        df["metric"].notna() & (df["metric"] != "") &
        df["source"].notna() & (df["source"] != "") &
        month_ok & df["value"].notna() & (df["value"] >= 0)
    )
    cleaned = df.loc[valid].drop_duplicates(
        subset=["state", "sector", "month", "metric"], keep="last"
    ).copy()
    rejected_rows = source_rows - len(cleaned)
    cleaned["ingested_at"] = datetime.now().isoformat(timespec="seconds")
    with connect() as conn:
        conn.execute("DELETE FROM records")
        cleaned.to_sql("records", conn, if_exists="append", index=False)
        summary = {
            "source_rows": int(source_rows),
            "clean_rows": int(len(cleaned)),
            "rejected_rows": int(rejected_rows),
            "duplicate_rows_removed": int(source_rows - int(valid.sum()) - (int(valid.sum()) - len(cleaned))),
            "sectors": int(cleaned["sector"].nunique()),
            "states": int(cleaned["state"].nunique()),
        }
        conn.execute(
            "INSERT INTO pipeline_runs (run_at, source_rows, clean_rows, rejected_rows, status, summary) VALUES (?, ?, ?, ?, ?, ?)",
            (datetime.now().isoformat(timespec="seconds"), source_rows, len(cleaned), rejected_rows, "Success", json.dumps(summary))
        )
    return summary

def get_dashboard_data():
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        states = conn.execute("SELECT COUNT(DISTINCT state) FROM records").fetchone()[0]
        sectors = conn.execute("SELECT COUNT(DISTINCT sector) FROM records").fetchone()[0]
        last_run = conn.execute("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 1").fetchone()
        sector_rows = conn.execute("""
            SELECT sector, ROUND(AVG(value), 2) AS avg_value, COUNT(*) AS records
            FROM records GROUP BY sector ORDER BY avg_value DESC
        """).fetchall()
        month_rows = conn.execute("""
            SELECT month, ROUND(AVG(value), 2) AS avg_value
            FROM records GROUP BY month ORDER BY month
        """).fetchall()
        state_rows = conn.execute("""
            SELECT state, sector, month, metric, value, source
            FROM records ORDER BY month DESC, state LIMIT 100
        """).fetchall()
        runs = conn.execute("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 5").fetchall()
    return {
        "total": total, "states": states, "sectors": sectors,
        "last_run": dict(last_run) if last_run else None,
        "sector_rows": [dict(r) for r in sector_rows],
        "month_rows": [dict(r) for r in month_rows],
        "records": [dict(r) for r in state_rows],
        "runs": [dict(r) for r in runs],
    }

@app.route("/")
def index():
    try:
        data = get_dashboard_data()
    except sqlite3.OperationalError:
        run_pipeline()
        data = get_dashboard_data()
    return render_template("index.html", data=data)

@app.route("/run-pipeline", methods=["POST"])
def pipeline():
    try:
        summary = run_pipeline()
        return jsonify({"ok": True, "message": "Pipeline completed successfully", "summary": summary})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500

@app.route("/api/records")
def api_records():
    with connect() as conn:
        rows = conn.execute("SELECT * FROM records ORDER BY month DESC, state").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/health")
def health():
    return jsonify({"ok": True, "app": "Economy Data Pipeline", "database": "sqlite"})

ensure_db()
create_sample_csv()
if __name__ == "__main__":
    # Seed clean data on first launch only; button can re-run the pipeline later.
    with connect() as conn:
        existing = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
    if existing == 0:
        run_pipeline()
    app.run(debug=True, host="127.0.0.1", port=5000)
