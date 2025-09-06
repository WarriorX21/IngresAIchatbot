import os
import pandas as pd

# --- CONFIGURATION ---
INPUT_FOLDER = 'yearly_reports'
OUTPUT_FILE = 'master_groundwater_data.csv'
# ---------------------

def get_year_and_header_index(file_path):
    """Finds the assessment year and the correct header row index from the Excel file."""
    year = None
    header_row_index = None
    try:
        df_header = pd.read_excel(file_path, header=None, nrows=15)
        for index, row in df_header.iterrows():
            if 'Assessment Year' in str(row.iloc[0]):
                year_range = str(row.iloc[1])
                year = year_range.split('-')[0]
            if str(row.iloc[0]).strip() == 'S.No':
                header_row_index = index
        return year, header_row_index
    except Exception:
        return None, None

def classify_extraction(stage):
    """Applies the official GEC rules to categorize the stage of extraction."""
    if pd.isna(stage) or not isinstance(stage, (int, float)):
        return 'No Data'
    
    if stage > 100:
        return 'Over-Exploited'
    elif 90 < stage <= 100:
        return 'Critical'
    elif 70 < stage <= 90:
        return 'Semi-Critical'
    else:
        return 'Safe'

def process_state_level_files():
    """
    Combines state-level yearly reports, uses the pre-calculated Stage of Extraction,
    creates the categorization column, and saves a final master CSV.
    """
    if not os.path.exists(INPUT_FOLDER):
        print(f"Error: Folder '{INPUT_FOLDER}' not found.")
        return

    all_dataframes = []
    files = [f for f in os.listdir(INPUT_FOLDER) if f.endswith('.xlsx')]
    print(f"Found {len(files)} Excel files to process...")

    for filename in files:
        file_path = os.path.join(INPUT_FOLDER, filename)
        year, header_idx = get_year_and_header_index(file_path)

        if not year or header_idx is None:
            print(f"Warning: Could not process metadata for '{filename}'. Skipping.")
            continue

        print(f"Processing '{filename}' for year {year}...")
        df = pd.read_excel(file_path, header=header_idx)
        df.columns = df.columns.str.strip()

        stage_column = 'Stage of Ground Water Extraction (%)'
        if stage_column not in df.columns:
            print(f"Warning: Column '{stage_column}' not found in {filename}. Skipping.")
            continue
            
        final_cols = {
            'STATE': 'State',
            'Annual Extractable Ground water Resource (ham)': 'Extractable_Resource_ham',
            'Ground Water Extraction for all uses (ha.m)': 'Extraction_ham',
            stage_column: 'Extraction_Percentage'
        }
        
        existing_cols = {k: v for k, v in final_cols.items() if k in df.columns}
        df_clean = df[list(existing_cols.keys())].copy()
        df_clean.rename(columns=existing_cols, inplace=True)

        df_clean['Extraction_Percentage'] = pd.to_numeric(df_clean['Extraction_Percentage'], errors='coerce')
        df_clean['Category'] = df_clean['Extraction_Percentage'].apply(classify_extraction)
        df_clean['Year'] = year
        all_dataframes.append(df_clean)

    if not all_dataframes:
        print("No data was successfully processed.")
        return

    master_df = pd.concat(all_dataframes, ignore_index=True).dropna(subset=['State'])
    master_df.to_csv(OUTPUT_FILE, index=False)
    
    print("\nSuccess!")
    print(f"All files have been combined and correctly processed into '{OUTPUT_FILE}'.")
    print(f"Total rows in master file: {len(master_df)}")

if __name__ == '__main__':
    process_state_level_files()

