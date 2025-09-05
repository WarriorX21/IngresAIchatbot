import os
import pandas as pd
import sqlite3

# --- CONFIGURATION ---
INPUT_FOLDER = 'yearly_reports'
DB_FILE_PATH = 'ingres.db'
TABLE_NAME = 'state_data'  # Here is where we explicitly define the table name
# ---------------------

def get_year_and_header_index(file_path):
    """Finds the assessment year and the correct header row index from the Excel file."""
    year, header_row_index = None, None
    try:
        df_header = pd.read_excel(file_path, header=None, nrows=15)
        for index, row in df_header.iterrows():
            if 'Assessment Year' in str(row.iloc[0]):
                year_range = str(row.iloc[1])
                year = year_range.split('-')[0]
            if str(row.iloc[0]).strip() == 'S.No':
                header_row_index = index
        return year, header_row_index
    except Exception as e:
        print(f"  - Could not read header from {os.path.basename(file_path)}: {e}")
        return None, None

def classify_extraction(stage):
    """Applies the official GEC rules to categorize the stage of extraction."""
    if pd.isna(stage) or not isinstance(stage, (int, float, str)):
        return 'No Data'
    try:
        stage_float = float(stage)
        if stage_float > 100: return 'Over-Exploited'
        if 90 < stage_float <= 100: return 'Critical'
        if 70 < stage_float <= 90: return 'Semi-Critical'
        return 'Safe'
    except (ValueError, TypeError):
        if isinstance(stage, str) and 'saline' in stage.lower():
            return 'Saline'
        return 'No Data'

def setup_database_from_reports():
    """
    Combines all yearly reports, creates the categorization, and saves everything
    directly into a new SQLite database file.
    """
    print("--- Starting Data and Database Setup ---")
    if not os.path.exists(INPUT_FOLDER):
        print(f"❌ Error: The input folder '{INPUT_FOLDER}' was not found.")
        return

    all_dataframes = []
    files = [f for f in os.listdir(INPUT_FOLDER) if f.endswith('.xlsx')]
    print(f"[1/3] Found {len(files)} Excel files to process...")

    for filename in files:
        file_path = os.path.join(INPUT_FOLDER, filename)
        year, header_idx = get_year_and_header_index(file_path)

        if not year or header_idx is None:
            print(f"  - Warning: Skipping '{filename}' (could not find year or header).")
            continue

        print(f"  - Processing '{filename}' for year {year}...")
        df = pd.read_excel(file_path, header=header_idx, engine='openpyxl')
        df.columns = df.columns.str.strip()

        stage_column = 'Stage of Ground Water Extraction (%)'
        if stage_column not in df.columns:
            print(f"    - Warning: Column '{stage_column}' not found. Skipping.")
            continue
            
        df_clean = df[['STATE', stage_column]].copy()
        df_clean.rename(columns={'STATE': 'State', stage_column: 'Extraction_Percentage'}, inplace=True)
        df_clean['Extraction_Percentage'] = pd.to_numeric(df_clean['Extraction_Percentage'], errors='coerce')
        df_clean['Category'] = df_clean['Extraction_Percentage'].apply(classify_extraction)
        df_clean['Year'] = year
        all_dataframes.append(df_clean)

    if not all_dataframes:
        print("❌ Error: No data was successfully processed from the Excel files.")
        return

    master_df = pd.concat(all_dataframes, ignore_index=True).dropna(subset=['State'])
    print(f"[2/3] Successfully combined data for {len(master_df)} records.")

    try:
        conn = sqlite3.connect(DB_FILE_PATH)
        master_df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
        print(f"[3/3] ✅ Success! Database '{DB_FILE_PATH}' is created and ready.")
    except Exception as e:
        print(f"❌ Error creating database: {e}")

if __name__ == '__main__':
    setup_database_from_reports()
