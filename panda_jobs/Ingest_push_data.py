
import os
import time
import requests
import pandas as pd
from pandas import json_normalize
from dotenv import load_dotenv
import snowflake.connector
from datetime import datetime

# Load env variables
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Get records from openFDA API
BASE_URL = "https://api.fda.gov/drug/shortages.json"
BATCH_SIZE = 100
all_records = []

print("Fetching all OpenFDA Drug Shortages records...")

skip = 0
while True:
    params = {"limit": BATCH_SIZE, "skip": skip}
    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        print(f"Request failed at skip={skip}: {response.status_code}")
        break

    data = response.json()
    results = data.get("results", [])

    if not results:
        print("No more results found.")
        break

    all_records.extend(results)
    skip += BATCH_SIZE
    print(f"Retrieved {len(all_records)} total records so far...")

    time.sleep(1)  # respect API rate limits

print(f"Total records fetched: {len(all_records)}")

# Flatten and clean JSON
if not all_records:
    raise ValueError("No data returned from API!")

df = json_normalize(all_records)
df.columns = [c.upper().replace(".", "_") for c in df.columns]
df = df.fillna("")

print(f"Flattened DataFrame shape: {df.shape}")

# Generate timestamped file + Table Name
timestamp = datetime.now().strftime("%Y_%m_%d")
csv_path = f"drug_shortages_{timestamp}.csv"
table_name = f"DRUG_SHORTAGES_RAW_{timestamp}"
master_view = "DRUG_SHORTAGES_MASTER_VIEW"

df.to_csv(csv_path, index=False)
print(f"Saved CSV locally as {csv_path}")

# 5 Connect to snowflake
print("Connecting to Snowflake...")

conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
)
cur = conn.cursor()

# Create table and match to CSV schema
print(f"Creating target table {table_name} with dynamic schema...")

columns_sql = ",\n    ".join([f'"{col}" STRING' for col in df.columns])
create_table_sql = f"""
CREATE OR REPLACE TABLE {table_name} (
    {columns_sql}
);
"""
cur.execute(create_table_sql)
print(f"Table {table_name} created successfully.")

#Upload CSV to snowflake
print("Uploading CSV to Snowflake stage...")

try:
    cur.execute(f"PUT file://{csv_path} @%{table_name} AUTO_COMPRESS=TRUE;")
    print("File uploaded to Snowflake internal stage.")

    print("Copying data into table...")
    cur.execute(f"""
        COPY INTO {table_name}
        FROM @%{table_name}
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY='"'
            SKIP_HEADER=1
        )
        ON_ERROR = 'CONTINUE';
    """)
    print(f"Data successfully loaded into {table_name}!")
except Exception as e:
    print("Error during COPY INTO:", e)

# Build and refresh master view
print("Refreshing Master View...")

# Get all versioned tables in schema
cur.execute("""
    SHOW TABLES LIKE 'DRUG_SHORTAGES_RAW_%';
""")
tables = [row[1] for row in cur.fetchall() if row[1].startswith("DRUG_SHORTAGES_RAW_")]

if tables:
    union_sql = "\nUNION ALL\n".join([f"SELECT * FROM {t}" for t in tables])
    view_sql = f"CREATE OR REPLACE VIEW {master_view} AS\n{union_sql};"
    cur.execute(view_sql)
    print(f"Master view {master_view} refreshed with {len(tables)} tables.")
else:
    print("No versioned tables found to build master view.")

# Close connection
cur.close()
conn.close()

print(f"Pipeline completed successfully! Table created: {table_name}")
print(f"Master view available: {master_view}")
