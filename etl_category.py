# etl_category.py
import pyodbc
import pandas as pd
from config import CONNECTION_STRING, REPORTING_CONNECTION_STRING

# --- 1. EXTRACT ---
def extract_categories():
    print("1. Extracting data from NORTHWND.Categories...")
    try:
        cnxn = pyodbc.connect(CONNECTION_STRING)
        # Select the columns needed for the dimension table
        sql_query = "SELECT CategoryID, CategoryName, Description FROM Categories"
        df = pd.read_sql(sql_query, cnxn)
        cnxn.close()
        print(f"   Extracted {len(df)} rows.")
        return df

    except pyodbc.Error as ex:
        print(f"Extraction failed: {ex}")
        return pd.DataFrame()

# --- 2. TRANSFORM ---
def transform_data(df):
    print("2. Transformation (Renaming and Cleaning)...")
    # Rename columns to match the Dim_Category table structure
    df.rename(columns={'CategoryID': 'Source_CategoryID',
                       'Description': 'CategoryDescription'},
              inplace=True)

    # Ensure all necessary columns exist (Source_CategoryID, CategoryName, CategoryDescription)
    df = df[['Source_CategoryID', 'CategoryName', 'CategoryDescription']]
    
    print("   Data transformed successfully.")
    return df

# --- 3. LOAD ---
def load_dimension_table(df):
    if df.empty:
        print("   No data to load. Skipping Load phase.")
        return

    # IMPORTANT: Target table is Dim_Category
    TARGET_TABLE = "Dim_Category" 

    print(f"3. Loading data into Northwind_Reporting_DB.{TARGET_TABLE}...")
    try:
        cnxn = pyodbc.connect(REPORTING_CONNECTION_STRING)
        cursor = cnxn.cursor()

        # Simple Load Strategy: Truncate and Reload (for dimensions that change slowly)
        cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE}")
        print(f"   Table {TARGET_TABLE} truncated.")
        
        # Prepare the INSERT statement matching the Dim_Category columns
        insert_sql = f"INSERT INTO {TARGET_TABLE} (Source_CategoryID, CategoryName, CategoryDescription) VALUES (?, ?, ?)"

        # Prepare data for fast insertion
        data_to_insert = [tuple(row) for row in df.values]
        
        # Use executemany for efficiency
        cursor.executemany(insert_sql, data_to_insert)
        
        cnxn.commit()
        cursor.close()
        cnxn.close()

        print(f"   ✅ Successfully loaded {len(df)} rows into {TARGET_TABLE}.")

    except pyodbc.Error as ex:
        print(f"Loading failed: {ex}")
        cnxn.rollback()


if __name__ == '__main__':
    categories_df = extract_categories()
    transformed_df = transform_data(categories_df)
    load_dimension_table(transformed_df)