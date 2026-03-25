import os

import snowflake.connector
from dotenv import load_dotenv

from base_adapter import BaseWarehouseAdapter

# Load the .env file from the root directory
load_dotenv()


class SnowflakeAdapter(BaseWarehouseAdapter):
    def __init__(self):
        """
        Initializes the Snowflake connection using environment variables.
        """
        try:
            self.ctx = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                role=os.getenv("SNOWFLAKE_ROLE"),
                # For local testing if you use SSO, uncomment the line below:
                # authenticator='externalbrowser'
            )
            self.cursor = self.ctx.cursor()
        except Exception as e:
            raise Exception(f"Failed to connect to Snowflake: {e}")

    def get_table_stats(self, db, schema, table):
        """
        Queries INFORMATION_SCHEMA for table size and last altered date.
        """
        # Snowflake is case-sensitive for string literals in queries
        db = db.upper()
        schema = schema.upper()
        table = table.upper()

        query = f"""
        SELECT
            BYTES,
            LAST_ALTERED
        FROM {db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table}'
        """

        try:
            self.cursor.execute(query)
            result = self.cursor.fetchone()

            if result:
                bytes_size, last_altered = result
                # Convert bytes to GB for human readability
                gb_size = round(bytes_size / (1024**3), 2) if bytes_size else 0
                return {
                    "exists": True,
                    "size_gb": gb_size,
                    "last_altered": str(last_altered),
                }
            else:
                return {"exists": False, "size_gb": 0, "last_altered": "N/A"}
        except Exception as e:
            print(f"Error querying Snowflake for {table}: {e}")
            return {"exists": False, "size_gb": 0, "last_altered": "Error"}

    def close(self):
        self.cursor.close()
        self.ctx.close()


# --- Local Test ---
if __name__ == "__main__":
    sf = SnowflakeAdapter()
    # Replace with a real table you know exists in your Snowflake account
    test_table = sf.get_table_stats("PROD", "STAGING", "STG_USERS")
    print(f"Test Result: {test_table}")
    sf.close()
