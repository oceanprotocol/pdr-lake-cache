#!/usr/bin/env python3
import os
import duckdb
import pickle
from datetime import datetime, timedelta
from collections import defaultdict

backup_folder = "exports"

def get_min_timestamp_from_db(table_name, db_path):
    conn = duckdb.connect(db_path)
    cursor = conn.cursor()
    timestamp_query = f"SELECT MIN(\"timestamp\") FROM {table_name};"
    cursor.execute(timestamp_query)
    last_timestamp = cursor.fetchone()[0]
    conn.close()
    return last_timestamp

def get_last_timestamp_from_db(table_name, db_path):
    conn = duckdb.connect(db_path)
    cursor = conn.cursor()
    timestamp_query = f"SELECT MAX(\"timestamp\") FROM {table_name};"
    cursor.execute(timestamp_query)
    last_timestamp = cursor.fetchone()[0]
    conn.close()
    return int(last_timestamp)

def read_last_export_timestamp(table_name, last_export_file):
    data = defaultdict(lambda: 0)
    try:
      if os.path.exists(last_export_file):
          with open(last_export_file, 'rb') as f:
              data = pickle.load(f)
    except Exception as e:
        pass
    return data[table_name]

# Function to update last export timestamp for a table
def update_last_export_timestamp(table_name, timestamp, last_export_file):
    with open(last_export_file, 'r+') as f:
        lines = f.readlines()
        f.seek(0)
        for line in lines:
            if not line.startswith(table_name):
                f.write(line)
        f.write(f"{table_name} {timestamp}\n")
        f.truncate()

def export_table(table_name, last_export_file, db_path):
    last_export_time = read_last_export_timestamp(table_name, last_export_file)
    if last_export_time == 0:
        print("got 0", last_export_time, table_name)
        last_export_time = get_min_timestamp_from_db(table_name, db_path)
    end_time = last_export_time + 86400000

    conn = duckdb.connect(db_path)
    cursor = conn.cursor()
    timestamp_query = f"SELECT MAX(\"timestamp\") FROM {table_name} WHERE \"timestamp\" >= {last_export_time} and \"timestamp\" <= {end_time};"
    cursor.execute(timestamp_query)
    last_timestamp = cursor.fetchone()[0]
    conn.close()
    print("last timestamp is:", last_timestamp)
    if last_timestamp is None:
        last_timestamp = end_time


    last_ts = get_last_timestamp_from_db(table_name, db_path)
    if last_ts - int(last_export_time) < 86400000:
        print("Skipping")
        return None
    os.makedirs(os.path.join(backup_folder, table_name), exist_ok=True)
    os.system(f"./duckdb -c \"COPY (SELECT * FROM {table_name} WHERE \"timestamp\" >= {last_export_time} and \"timestamp\" <= {last_timestamp}) TO '{table_name}.parquet' (FORMAT PARQUET);\" {db_path}")


    filename = f"{table_name}-{last_export_time}-{end_time}.parquet"
    print("Exporting:", filename)
    

    try:
      os.rename(f"{table_name}.parquet", os.path.join(backup_folder, table_name, filename))
    except Exception as e:
      return end_time

    return end_time


def main():
  os.makedirs(backup_folder, exist_ok=True)


  db_path = '../pdr-backend/lake_data/duckdb.db'
  last_export_file = 'last_export_timestamps'

  duckdb.connect(db_path)

  export_data = {}
  tables = ["bronze_pdr_predictions", "bronze_pdr_predictions", "pdr_payouts", "pdr_predictions", "pdr_truevals"]
  for table in tables:
      ts_last = export_table(table, last_export_file, db_path)
      if ts_last is None:
          export_data[table] = read_last_export_timestamp(table, last_export_file)
          continue
      export_data[table] = ts_last


  print(export_data)
  with open(last_export_file, 'wb') as f:
      pickle.dump(export_data, f)

  print("Done")

main()
