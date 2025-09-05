import sqlite3
import pandas as pd

# --- CONFIGURATION ---
CSV_FILE_PATH = 'master_groundwater_data.csv'
DB_FILE_PATH = 'ingres.db'
TABLE_NAME = 'yearly_state_data'
# ---------------------

def create_database_from_master_csv():
    """
    Reads the master CSV and creates a clean SQLite database from it.
    """
    try:
        df = pd.read_csv(CSV_FILE_PATH, low_memory=False)
        print(f"Successfully loaded {len(df)} rows from {CSV_FILE_PATH}")
    except FileNotFoundError:
        print(f"Error: The file '{CSV_FILE_PATH}' was not found.")
        return

    # --- Select and Clean the Core Columns ---
    # We select only the columns we need for the chatbot to make the DB efficient.
    required_columns = {
        'STATE': 'State',
        'Year': 'Year',
        'Annual Extractable Ground water Resource (ham)': 'Extractable_Resource_ham',
        'Ground Water Extraction for all uses (ha.m)': 'Extraction_ham',
        'Stage of Ground Water Extraction (%)': 'Extraction_Percentage',
        'Categorisation': 'Category'
    }
    
    # Filter for only the columns that exist in the dataframe
    existing_cols = {k: v for k, v in required_columns.items() if k.strip() in [c.strip() for c in df.columns]}
    df = df[[k.strip() for k in existing_cols.keys()]]
    df.rename(columns={k.strip(): v for k, v in existing_cols.items()}, inplace=True)

    # Connect to the SQLite database (creates the file if it doesn't exist)
    conn = sqlite3.connect(DB_FILE_PATH)
    print(f"Connected to the database at '{DB_FILE_PATH}'")

    # Insert the data into the database table
    df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()

    print(f"\nSuccess! ✨")
    print(f"All data has been imported into the '{TABLE_NAME}' table in '{DB_FILE_PATH}'.")
    print("Your database is now ready for the chatbot.")

if __name__ == '__main__':
    create_database_from_master_csv()