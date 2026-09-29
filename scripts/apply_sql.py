"""Run a .sql file against the warehouse (no psql needed - works on Windows, CI and cloud DBs).
Usage: python scripts/apply_sql.py dbt/schema.sql
Connection comes from PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE (and PGSSLMODE for cloud DBs)."""
import os
import sys
from sqlalchemy import create_engine

url = "postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}".format(
    u=os.environ.get("PGUSER", "mediapulse"), p=os.environ.get("PGPASSWORD", "mediapulse_dev_pw"),
    h=os.environ.get("PGHOST", "localhost"), port=os.environ.get("PGPORT", "5432"),
    d=os.environ.get("PGDATABASE", "mediapulse"))
sql = open(sys.argv[1], encoding="utf-8").read()
with create_engine(url).begin() as conn:
    conn.exec_driver_sql(sql)
print(f"applied {sys.argv[1]}")
