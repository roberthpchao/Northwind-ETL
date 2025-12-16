## ⏭️ Next Step: Phase 5: ETL Pipeline (Order Fact)

Now we move on to the most crucial and technically advanced part of the Data Mart project: **loading the central `Fact_OrderMetrics` table.**

This phase requires two key actions:

1.  **Lookups:** Reading the data from the source, performing calculations, and most importantly, looking up the new `ProductKey` and `CategoryKey` from our newly loaded dimension tables.
2.  **Mapping:** Using the `Source_ProductID` from the source data to find the corresponding `ProductKey` in `Dim_Product`.

### Step 1: Write the `etl_orders.py` Script (Fact Table Logic)

Create a new file named `etl_orders.py`. This script will contain the logic to join the source tables, perform the lookups, and load the metrics.

```python
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
        # Select the Source Key and the Surrogate Key from our loaded Dim table
        product_map_query = "SELECT ProductKey, Source_ProductID FROM Dim_Product"
        product_map_df = pd.read_sql(product_map_query, cnxn)
        cnxn.close()
    except pyodbc.Error as ex:
        print(f"   Failed to load dimension map: {ex}")
        return pd.DataFrame()

    # C. Perform the Lookup (Merge)
    # Join the Fact data (fact_df) with the Dimension Map (product_map_df) on the shared natural key (Source_ProductID)
    transformed_df = pd.merge(
        fact_df, 
        product_map_df, 
        left_on='ProductID', 
        right_on='Source_ProductID', 
        how='left' # Use left join to keep all fact rows
    )
    
    # D. Final Clean-up and Renaming
    transformed_df.rename(columns={
        'ProductID': 'Source_ProductID',
        'OrderID': 'Source_OrderID',
    }, inplace=True)
    
    # Select and reorder columns to match the Fact_OrderMetrics table
    final_cols = [
        'Source_OrderID', 'Source_ProductID', 'ProductKey', 
        'OrderDate', 'RequiredDate', 'ShippedDate', 
        'Quantity', 'Discount', 'ExtendedPrice'
    ]
    transformed_df = transformed_df[final_cols]

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
        # Since Fact tables can be large, we typically use partitioning or incremental load, 
        # but for this project, we'll use DELETE for simplicity.
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

        # Prepare data for fast insertion
        data_to_insert = [tuple(row) for row in df.values]
        
        cursor.executemany(insert_sql, data_to_insert)
        
        cnxn.commit()
        cursor.close()
        cnxn.close()

        print(f"   ✅ Successfully loaded {len(df)} rows into {TARGET_TABLE}.")

    except pyodbc.Error as ex:
        print(f"Loading failed: {ex}")
        cnxn.rollback()


if __name__ == '__main__':
    order_fact_df = extract_order_data()
    transformed_fact_df = transform_fact_data(order_fact_df)
    load_fact_table(transformed_fact_df)
```

### Step 2: Run and Verify

1.  **Run the script:**
    ```bash
    (venv_nw) python etl_orders.py
    ```
2.  **Verify the results in SSMS:**
    ```sql
    -- 1. Check the total number of records (should be 2155 rows)
    SELECT COUNT(*) FROM Fact_OrderMetrics; 

    -- 2. Verify the lookups worked (ProductKey should NOT be NULL)
    SELECT TOP 10 * FROM Fact_OrderMetrics WHERE ProductKey IS NULL; -- Should return 0 rows

    -- 3. Verify the calculated metric
    SELECT TOP 10 ExtendedPrice, Quantity, Discount FROM Fact_OrderMetrics ORDER BY ExtendedPrice DESC;
    ```


---

## 3. Loading data into Northwind_Reporting_DB.Fact_OrderMetrics...
   Table Fact_OrderMetrics cleared using DELETE.
Loading failed: ('The SQL contains 9 parameter markers, but 10 parameters were supplied', 'HY000')

This is a very specific and easily fixable error related to how Python's `pyodbc` handles data being sent from a Pandas DataFrame to the SQL Server.

The error message: `('The SQL contains 9 parameter markers, but 10 parameters were supplied', 'HY000')` means:

1.  **The SQL Statement:** Your `INSERT` statement has **9 question marks** (`?`):
    ```sql
    ... VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) -- 9 question marks
    ```
2.  **The Data:** The data you are trying to insert (`data_to_insert`) has **10 columns** per row.

### 🔍 The Cause: Hidden Index Column in Pandas

When you convert a Pandas DataFrame to a list of tuples using standard methods like `df.values` and then iterate over them, the **DataFrame's index is often included as the first element in each row tuple**, resulting in one extra column that the SQL statement isn't expecting.

### 📝 The Fix: Exclude the Index

We need to explicitly tell Pandas **NOT** to include the index when converting the DataFrame into the list of rows for insertion.

**Update the `load_fact_table` function in your `etl_orders.py` file with the highlighted change:**

```python
# etl_orders.py - Update the LOAD function only

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
```

### 🎯 Next Step: Run the Script

Please update the `load_fact_table` function in `etl_orders.py` and run the script one more time:

```bash
(venv_nw) python etl_orders.py
```

----

##3. Loading data into Northwind_Reporting_DB.Fact_OrderMetrics...
   Table Fact_OrderMetrics cleared using DELETE.
Traceback (most recent call last):
  File "c:\Users\Admin\Documents\Projects\Northwind_ETL\etl_orders.py", line 135, in <module>
    load_fact_table(transformed_fact_df)
    ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^
  File "c:\Users\Admin\Documents\Projects\Northwind_ETL\etl_orders.py", line 116, in load_fact_table
    raise ValueError(f"Data row has {len(data_to_insert[0])} columns, but SQL expects 9.")
ValueError: Data row has 10 columns, but SQL expects 9.

Thank you for providing the updated traceback\! This is very precise and tells us exactly where the issue lies now.

The script is now correctly diagnosing the problem, but we haven't eliminated the extra column yet:

The error: `ValueError: Data row has 10 columns, but SQL expects 9.`

This confirms that even with the `df.to_numpy()` fix, one extra column is still present in the data being passed to the SQL Server. This typically happens when **an extra column was created during the Extract or Transform steps and was not included in the final column list.**

### 🔍 The Cause: Unwanted Column Leftover

Let's re-examine the `transform_fact_data` function in `etl_orders.py`.

In the transformation step, you perform a `pd.merge` to link the fact data with the product map:

```python
transformed_df = pd.merge(
    fact_df, 
    product_map_df, 
    left_on='ProductID', 
    right_on='Source_ProductID', 
    how='left' 
)
```

The `product_map_df` contains two columns: `ProductKey` and `Source_ProductID`. When you merge, both are added to `transformed_df`.

Later, you clean up and select the final columns:

```python
final_cols = [
    'Source_OrderID', 'Source_ProductID', 'ProductKey', 
    'OrderDate', 'RequiredDate', 'ShippedDate', 
    'Quantity', 'Discount', 'ExtendedPrice'
]
transformed_df = transformed_df[final_cols]
```

**The Bug:** The DataFrame being inserted (`transformed_df`) still contains the original `Source_ProductID` **and** the merged `Source_ProductID` column (or one was renamed incorrectly, leaving a duplicate name in the mix, or simply not excluded).

### 📝 The Fix: Explicit Column Exclusion

We need to ensure that the intermediate columns used for the merge are **dropped** before the final column selection to prevent accidental inclusion.

**Update the `transform_fact_data` function in your `etl_orders.py` file:**

```python
# etl_orders.py - Update the TRANSFORM function only

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
        # Select the Source Key and the Surrogate Key from our loaded Dim table
        product_map_query = "SELECT ProductKey, Source_ProductID FROM Dim_Product"
        product_map_df = pd.read_sql(product_map_query, cnxn)
        cnxn.close()
    except pyodbc.Error as ex:
        print(f"   Failed to load dimension map: {ex}")
        return pd.DataFrame()

    # C. Perform the Lookup (Merge)
    # Join the Fact data (fact_df) with the Dimension Map (product_map_df) on the shared natural key (ProductID in fact_df)
    transformed_df = pd.merge(
        fact_df, 
        product_map_df, 
        left_on='ProductID', 
        right_on='Source_ProductID', 
        how='left' 
    )
    
    # D. Final Clean-up and Renaming
    
    # CRITICAL FIX: The merge adds Source_ProductID from the map. We only need the original ProductID 
    # (renamed to Source_ProductID later) and the ProductKey. Drop the extra Source_ProductID column added by the merge.
    transformed_df.drop('Source_ProductID_y', axis=1, inplace=True)
    
    # Rename the original source ID (which is now Source_ProductID_x)
    transformed_df.rename(columns={
        'ProductID': 'Source_ProductID',
        'OrderID': 'Source_OrderID',
        'Source_ProductID_x': 'Source_ProductID' # Rename the original source ID back to the desired name
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

# ... (rest of the file remains the same)
```

**Explanation of the fix:** When Pandas performs a merge using columns with the same name, it renames them (e.g., `Source_ProductID_x` and `Source_ProductID_y`). We only need one of the source IDs (`Source_ProductID_x`) and the new key (`ProductKey`). The fix explicitly removes the redundant `Source_ProductID_y` column.

### 🎯 Next Step: Run the Script

Please update the `transform_fact_data` function in `etl_orders.py` with the changes above and run the script one last time:

```bash
(venv_nw) python etl_orders.py
```

This should finally resolve the column count issue. Let me know the success message\!

-----------

##2. Transformation: Calculating ExtendedPrice and performing Key Lookups...
   Loading Product Key mapping for lookup...
c:\Users\Admin\Documents\Projects\Northwind_ETL\etl_orders.py:50: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  product_map_df = pd.read_sql(product_map_query, cnxn)
Traceback (most recent call last):
  File "c:\Users\Admin\Documents\Projects\Northwind_ETL\etl_orders.py", line 144, in <module>
    transformed_fact_df = transform_fact_data(order_fact_df)
  File "c:\Users\Admin\Documents\Projects\Northwind_ETL\etl_orders.py", line 70, in transform_fact_data
    transformed_df.drop('Source_ProductID_y', axis=1, inplace=True)
    ~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Admin\Documents\Projects\Northwind_ETL\venv_nw\Lib\site-packages\pandas\core\frame.py", line 5603, in drop
    return super().drop(
           ~~~~~~~~~~~~^
        labels=labels,
        ^^^^^^^^^^^^^^
    ...<5 lines>...
        errors=errors,
        ^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\Admin\Documents\Projects\Northwind_ETL\venv_nw\Lib\site-packages\pandas\core\generic.py", line 4810, in drop
    obj = obj._drop_axis(labels, axis, level=level, errors=errors)
  File "C:\Users\Admin\Documents\Projects\Northwind_ETL\venv_nw\Lib\site-packages\pandas\core\generic.py", line 4852, in _drop_axis
    new_axis = axis.drop(labels, errors=errors)
  File "C:\Users\Admin\Documents\Projects\Northwind_ETL\venv_nw\Lib\site-packages\pandas\core\indexes\base.py", line 7136, in drop
    raise KeyError(f"{labels[mask].tolist()} not found in axis")
KeyError: "['Source_ProductID_y'] not found in axis"

---

The current error, `KeyError: "['Source_ProductID_y'] not found in axis"`, tells us that the previous fix, which attempted to drop a column named `'Source_ProductID_y'`, failed because that column name **did not exist** in the DataFrame after the merge.

### 🔍 The Cause: Pandas Merge Naming

Here is why the column name was different than expected:

  * **Before Merge:**
      * `fact_df` had a column named: `ProductID`
      * `product_map_df` had a column named: `Source_ProductID`
  * **During Merge:** You merged using `left_on='ProductID'` and `right_on='Source_ProductID'`.
  * **Result of Merge:** Since the key columns used for the join had **different names**, Pandas **does not** automatically add `_x` and `_y` suffixes. Instead, the resulting DataFrame keeps both original columns: `ProductID` (from the left side) and `Source_ProductID` (from the right side).

Therefore, the redundant column that needs to be dropped is **`Source_ProductID`** (the one from the lookup map), not `Source_ProductID_y`.

### 📝 The Final Fix: Correcting the Drop Command

We need to change the column we are dropping and ensure the original `ProductID` is renamed correctly to `Source_ProductID`.

**Update the `transform_fact_data` function in your `etl_orders.py` file one last time:**

```python
# etl_orders.py - Update the TRANSFORM function only

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

# ... (rest of the file remains the same)
```

### 🎯 Next Step: Run the Script

Please implement these two critical fixes in your `transform_fact_data` function and run the script:

```bash
(venv_nw) python etl_orders.py
```
