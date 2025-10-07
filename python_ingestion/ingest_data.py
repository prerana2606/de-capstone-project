import os
import json
import base64
import requests
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
print("Loading environment variables...")
load_dotenv()

# Snowflake credentials
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Fetch data from API
print("Fetching data from API...")
posts = requests.get("https://jsonplaceholder.typicode.com/posts").json()
comments = requests.get("https://jsonplaceholder.typicode.com/comments").json()
print(f"Fetched {len(posts)} posts and {len(comments)} comments.")

# Connect to Snowflake
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    authenticator='snowflake'
)
cursor = conn.cursor()
print("Connection successful.")

# Insert data into Snowflake
def insert_json_data(table_name, data):
    print(f"Inserting data into {table_name}...")
    select_statements = []
    for record in data:
        record_json = json.dumps(record)
        encoded = base64.b64encode(record_json.encode('utf-8')).decode('utf-8')
        select_statements.append(f"SELECT PARSE_JSON(BASE64_DECODE_STRING('{encoded}'))")
    union_selects = " UNION ALL ".join(select_statements)
    insert_query = f"INSERT INTO {table_name} (raw_json) {union_selects}"
    cursor.execute(insert_query)
    print(f"Inserted {len(data)} records into {table_name}.")

# Load into POSTS and COMMENTS tables
insert_json_data(f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.POSTS", posts)
insert_json_data(f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COMMENTS", comments)

# Close connections
cursor.close()
conn.close()
print("All tasks completed. Connection closed.")
