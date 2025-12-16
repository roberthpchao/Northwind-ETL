# etl_product.py
import pyodbc
import pandas as pd
from config import CONNECTION_STRING, REPORTING_CONNECTION_STRING

# --- 1. EXTRACT ---
def extract_products():
    print("1. Extracting data from NORTHWND.Products...")
    try:
        cnxn = pyodbc.connect(CONNECTION_STRING)
        # We select all relevant columns from the source Products table
        sql_query = """
        SELECT 
            ProductID, 
            ProductName, 
            SupplierID,
            CategoryID, 
            QuantityPerUnit,
            UnitPrice,
            UnitsInStock,
            Discontinued
        FROM Products
        """
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
    
    # Rename columns to match the Dim_Product table structure and convention
    df.rename(columns={
        'ProductID': 'Source_ProductID',
        'SupplierID': 'Source_SupplierID',
        'CategoryID': 'Source_CategoryID'
    }, inplace=True)

    # Simple data type correction/cleaning (Good practice, even if MSSQL is usually good with these)
    df['Source_ProductID'] = pd.to_numeric(df['Source_ProductID'], errors='coerce').astype('Int64')
    df['UnitPrice'] = pd.to_numeric(df['UnitPrice'], errors='coerce')

    # Reorder columns to match the Dim_Product table structure for insertion
    df = df[[
        'Source_ProductID', 
        'ProductName', 
        'Source_SupplierID', 
        'Source_CategoryID', 
        'QuantityPerUnit',
        'UnitPrice',
        'UnitsInStock',
        'Discontinued'
    ]]
    
    print("   Data transformed successfully.")
    return df

# --- 3. LOAD ---
def load_dimension_table(df):
    if df.empty:
        print("   No data to load. Skipping Load phase.")
        return

    TARGET_TABLE = "Dim_Product" 
    REFERENCING_TABLE = "Fact_OrderMetrics" # Table that has a foreign key to Dim_Product

    print(f"3. Loading data into Northwind_Reporting_DB.{TARGET_TABLE}...")
    try:
        cnxn = pyodbc.connect(REPORTING_CONNECTION_STRING)
        cursor = cnxn.cursor()

    # Simple Load Strategy: DELETE and Reload
        # DELETE bypasses the Foreign Key TRUNCATE restriction.
        cursor.execute(f"DELETE FROM {TARGET_TABLE}")
        print(f"   Table {TARGET_TABLE} cleared using DELETE.")
        
        # Prepare the INSERT statement matching the Dim_Product columns (8 columns)
        insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            Source_ProductID, ProductName, Source_SupplierID, Source_CategoryID, 
            QuantityPerUnit, UnitPrice, UnitsInStock, Discontinued
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Prepare data for fast insertion (converting DataFrame rows to list of tuples)
        # Note: We use the .values and list/tuple conversion for speed with executemany
        data_to_insert = [tuple(row) for row in df.values]
        
        # Use executemany for efficient batch insertion
        cursor.executemany(insert_sql, data_to_insert)
        
        cnxn.commit()
        cursor.close()
        cnxn.close()

        print(f"   ✅ Successfully loaded {len(df)} rows into {TARGET_TABLE}.")

    except pyodbc.Error as ex:
        print(f"Loading failed: {ex}")
        cnxn.rollback()


if __name__ == '__main__':
    products_df = extract_products()
    transformed_df = transform_data(products_df)
    load_dimension_table(transformed_df)