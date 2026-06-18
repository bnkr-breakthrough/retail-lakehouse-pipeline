import yaml
import snowflake.connector


# Load config
with open("../config/snowflake_config.yaml", "r") as file:
    config = yaml.safe_load(file)


# Connect
conn = snowflake.connector.connect(
    user=config["user"],
    password=config["password"],
    account=config["account"],
    warehouse=config["warehouse"],
    database=config["database"],
    role=config["role"]
)

print("Snowflake Connection Successful")

# Verify
cursor = conn.cursor()

cursor.execute("SELECT CURRENT_VERSION()")

result = cursor.fetchone()

print("Snowflake Version:", result[0])

cursor.close()
conn.close()
