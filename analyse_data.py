import pandas as pd

# --- CONFIGURATION ---
MASTER_CSV_FILE = 'master_groundwater_data.csv'
# ---------------------

def analyze_groundwater_data():
    """
    Reads the master CSV file, groups data by year, and counts the
    number of assessment units in each category for each year.
    """
    try:
        df = pd.read_csv(MASTER_CSV_FILE, low_memory=False)
        print(f"Successfully loaded '{MASTER_CSV_FILE}' with {len(df)} total records.")
    except FileNotFoundError:
        print(f"Error: The file '{MASTER_CSV_FILE}' was not found.")
        return

    # Standardize column names by stripping extra spaces
    df.columns = df.columns.str.strip()
    
    # --- THIS IS THE FIX ---
    # The column name in the master CSV is 'Category', not 'Categorisation'.
    category_column = 'Category'

    if category_column not in df.columns:
        print(f"\nError: The column '{category_column}' was not found.")
        print("Please check the headers in your 'master_groundwater_data.csv'.")
        print(f"Available columns are: {df.columns.tolist()}")
        return
        
    df[category_column] = df[category_column].str.strip()

    yearly_summary = df.groupby('Year')[category_column].value_counts().unstack(fill_value=0)

    print("\n--- Yearly Groundwater Assessment Summary ---")

    for year, data in yearly_summary.iterrows():
        print(f"\n📊 Assessment Year: {year}")
        print("-" * 30)
        total_units = int(data.sum())
        print(f"Total Assessed Units: {total_units}")
        
        # Print counts for each category
        for cat_name, count in data.items():
            if count > 0:
                print(f"- {cat_name}: {int(count)}")

if __name__ == '__main__':
    analyze_groundwater_data()
