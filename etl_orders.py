# etl_orders.py
import pyodbc
import pandas as pd
from config import CONNECTION_STRING, REPORTING_CONNECTION_STRING

# --- 1. EXTRACT ---
def extract_order_data():
    print("1. Extracting data from NORTHWND (Orders and Order Details)...")
    try:
        cnxn = pyodbc.connect(CONNECTION_STRING)
        # SQL to join Orders and Order Details and select required fields
        sql_query = """
        SELECT
            OD.OrderID, 
            OD.ProductID,
            O.OrderDate,
            O.RequiredDate,
            O.ShippedDate,
            OD.Quantity,
            OD.Discount,
            OD.UnitPrice -- Keep source UnitPrice for calculation
        FROM [Order Details] OD
        JOIN Orders O ON OD.OrderID = O.OrderID
        """
        df = pd.read_sql(sql_query, cnxn)
        cnxn.close()
        print(f"   Extracted {len(df)} order detail rows.")
        return df

    except pyodbc.Error as ex:
        print(f"Extraction failed: {ex}")
        return pd.DataFrame()

# --- 2. TRANSFORMATION (Lookups and Calculation) ---
def transform_fact_data(fact_df):
    print("2. Transformation: Calculating ExtendedPrice and performing Key Lookups...")

    # A. Calculate the ExtendedPrice metric
    fact_df['ExtendedPrice'] = fact_df['Quantity'] * fact_df['UnitPrice'] * (1 - fact_df['Discount'])
    
    # Drop the temporary calculation column
    fact_df.drop('UnitPrice', axis=1, inplace=True)
    
    # B. Load the Product Dimension Map for Lookup
    print("   Loading Product Key mapping for lookup...")
    try:
        cnxn = pyodbc.connect(REPORTING_CONNECTION_STRING)
        product_map_query = "SELECT ProductKey, Source_ProductID FROM Dim_Product"
        product_map_df = pd.read_sql(product_map_query, cnxn)
        cnxn.close()
    except pyodbc.Error as ex:
        print(f"   Failed to load dimension map: {ex}")
        return pd.DataFrame()

    # C. Perform the Lookup (Merge)
    # Join the Fact data (fact_df) with the Dimension Map (product_map_df) on the shared natural key
    transformed_df = pd.merge(
        fact_df, 
        product_map_df, 
        left_on='ProductID', 
        right_on='Source_ProductID', 
        how='left' 
    )
    
    # D. Final Clean-up and Renaming
    
    # CRITICAL FIX 1: Drop the redundant Source_ProductID column added by the merge 
    # (since the join columns had different names, no _x/_y suffixes were added)
    transformed_df.drop('Source_ProductID', axis=1, inplace=True)
    
    # CRITICAL FIX 2: Rename the original source IDs to the final names
    transformed_df.rename(columns={
        'ProductID': 'Source_ProductID', # Rename original source ID
        'OrderID': 'Source_OrderID',
    }, inplace=True)
    
    # Select and reorder columns to match the Fact_OrderMetrics table (ensuring only 9 columns)
    final_cols = [
        'Source_OrderID', 'Source_ProductID', 'ProductKey', 
        'OrderDate', 'RequiredDate', 'ShippedDate', 
        'Quantity', 'Discount', 'ExtendedPrice'
    ]
    transformed_df = transformed_df[final_cols]
    
    # Final check: Must have exactly 9 columns
    print(f"   Data transformed. Final column count: {len(transformed_df.columns)}") 

    print("   Data transformed and keys looked up successfully.")
    return transformed_df

# --- 3. LOAD ---
def load_fact_table(df):
    if df.empty:
        print("   No data to load. Skipping Load phase.")
        return

    TARGET_TABLE = "Fact_OrderMetrics" 

    print(f"3. Loading data into Northwind_Reporting_DB.{TARGET_TABLE}...")
    try:
        cnxn = pyodbc.connect(REPORTING_CONNECTION_STRING)
        cursor = cnxn.cursor()

        # Clear the Fact table
        cursor.execute(f"DELETE FROM {TARGET_TABLE}")
        print(f"   Table {TARGET_TABLE} cleared using DELETE.")
        
        # Prepare the INSERT statement matching the Fact_OrderMetrics columns (9 columns)
        insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            Source_OrderID, Source_ProductID, ProductKey, 
            OrderDate, RequiredDate, ShippedDate, 
            Quantity, Discount, ExtendedPrice
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Prepare data for fast insertion:
        # CRITICAL FIX: Use .to_numpy() to get the underlying array without the index, 
        # then convert to a list of tuples.
        data_to_insert = [tuple(row) for row in df.to_numpy()] # <-- CHANGE IS HERE
        
        # Check to confirm the number of columns matches before execution (Optional, but good diagnostic)
        if data_to_insert and len(data_to_insert[0]) != 9:
             raise ValueError(f"Data row has {len(data_to_insert[0])} columns, but SQL expects 9.")

        cursor.executemany(insert_sql, data_to_insert)
        
        cnxn.commit()
        cursor.close()
        cnxn.close()

        print(f"   ✅ Successfully loaded {len(df)} rows into {TARGET_TABLE}.")

    except pyodbc.Error as ex:
        # Added pyodbc.Error check to capture the specific error
        print(f"Loading failed: {ex}")
        cnxn.rollback()


if __name__ == '__main__':
    order_fact_df = extract_order_data()
    transformed_fact_df = transform_fact_data(order_fact_df)
    load_fact_table(transformed_fact_df)