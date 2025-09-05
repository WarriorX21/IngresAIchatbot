import sqlite3
import pandas as pd

# --- CONFIGURATION ---
DB_FILE_PATH = 'ingres.db'
TABLE_NAME = 'state_data'
# ---------------------

def verify_data():
    """
    Connects to the database and runs a few sample queries to verify its contents.
    """
    print(f"--- - Verifying Database: '{DB_FILE_PATH}' ---")
    try:
        conn = sqlite3.connect(DB_FILE_PATH)
        
        # Test 1: Check if the table exists and get its schema
        print(f"\n[1] Checking schema for table '{TABLE_NAME}'...")
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({TABLE_NAME});")
        schema = cursor.fetchall()
        if not schema:
            print(f"❌ Error: Table '{TABLE_NAME}' not found in the database.")
            return
        print("✅ Schema found. Columns:")
        for col in schema:
            print(f"  - {col[1]} ({col[2]})")

        # Test 2: Fetch a specific, known data point
        print("\n[2] Fetching data for Punjab in 2022...")
        query_2022 = f"SELECT * FROM {TABLE_NAME} WHERE State = 'PUNJAB' AND Year = 2022"
        df_2022 = pd.read_sql_query(query_2022, conn)
        
        if not df_2022.empty:
            print("✅ Data for Punjab (2022) found:")
            print(df_2022.to_string(index=False))
        else:
            print("❌ Data for Punjab (2022) NOT found.")

        # Test 3: Fetch all data for the year 2024 to test summary queries
        print("\n[3] Fetching all data for the year 2024...")
        query_2024 = f"SELECT State, Category, Extraction_Percentage FROM {TABLE_NAME} WHERE Year = 2024"
        df_2024 = pd.read_sql_query(query_2024, conn)

        if not df_2024.empty:
            print(f"✅ Found {len(df_2024)} records for 2024:")
            print(df_2024.to_string(index=False))
        else:
            print("❌ No data for 2024 was found.")

        conn.close()

    except Exception as e:
        print(f"\n❌ An error occurred: {e}")

if __name__ == '__main__':
    verify_data()
