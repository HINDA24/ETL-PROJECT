import os
import sqlite3
import pandas as pd

DATA_DIR = "data"
DB_PATH = "production.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    return conn

def create_tables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            record_id TEXT PRIMARY KEY,
            timestamp DATETIME,
            line_id TEXT,
            machine_id TEXT,
            temperature REAL,
            pressure REAL,
            vibration REAL,
            power REAL,
            data_quality TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS quality_checks (
            check_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            line_id TEXT,
            machine_id TEXT,
            result TEXT,
            defect_type TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hourly_summary (
            summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            hour DATETIME,
            line_id TEXT,
            machine_id TEXT,
            avg_temperature REAL,
            min_temperature REAL,
            max_temperature REAL,
            avg_pressure REAL,
            avg_vibration REAL,
            total_checks INTEGER,
            defect_count INTEGER,
            defect_rate REAL
        );
    """)
    conn.commit()

def load_sensor_readings(conn, csv_path=os.path.join(DATA_DIR, "standardized_sensor_data.csv")):
    if not os.path.exists(csv_path):
        print(f"[LOAD] Fichier introuvable : {csv_path}")
        return
    df = pd.read_csv(csv_path)
    df.to_sql("sensor_readings", conn, if_exists="replace", index=False)

def load_quality_checks(conn, csv_path=os.path.join(DATA_DIR, "quality_data.csv")):
    if not os.path.exists(csv_path):
        print(f"[LOAD] Fichier introuvable : {csv_path}")
        return
    df = pd.read_csv(csv_path)
    cols = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df.columns = cols
    needed = ["timestamp", "line_id", "machine_id", "result", "defect_type"]
    for col in needed:
        if col not in df.columns:
            df[col] = None
    df[needed].to_sql("quality_checks", conn, if_exists="replace", index=False)

def load_hourly_summary(conn, csv_path=os.path.join(DATA_DIR, "hourly_summary.csv")):
    if not os.path.exists(csv_path):
        print(f"[LOAD] Fichier introuvable : {csv_path}")
        return
    df = pd.read_csv(csv_path)
    df.to_sql("hourly_summary", conn, if_exists="replace", index=False)
