The error message `Cannot find the object "Staging_Categories"` means that your Python script is successfully connecting to the SQL Server and the target database (`Northwind_Reporting_DB`), but **the table it is trying to write data to does not exist in that database.**

You are attempting to load into `Staging_Categories`, but based on our revised plan, we should be loading into the final dimension table: **`Dim_Category`**. It's highly likely you did not run the SQL code in SSMS yet to create the new schema tables.

Here is the plan to fix this, followed by an updated `etl_category.py` script.

### 📝 Fix Steps: Table Creation and Script Update

#### **Step 1: Address the Warning (Informational)**

The warning: `UserWarning: pandas only supports SQLAlchemy connectable...` is from pandas suggesting you use `SQLAlchemy` for complex operations. **You can safely ignore this warning** for our current ETL script, as `pd.read_sql` works fine with a simple `pyodbc` connection object for basic extraction.

#### **Step 2: Create the Target Table (SSMS)**

Go back to **SSMS**, ensure you are connected, select the **`Northwind_Reporting_DB`** in the dropdown, and run the following SQL code to create the dimension table structure from our revised plan:

```sql
-- In Northwind_Reporting_DB
CREATE TABLE Dim_Category (
    CategoryKey INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    Source_CategoryID INT NOT NULL UNIQUE,     -- Source System Key
    CategoryName NVARCHAR(15) NOT NULL,
    CategoryDescription NTEXT
);

-- OPTIONAL: Verify creation
SELECT * FROM Dim_Category;
```

#### **Step 3: Update the Python Script (Refine Load Logic)**

Since we are now loading into `Dim_Category` and not a temporary staging table, we need to update the load function to use the correct table name and column list.

**Replace the content of your `etl_category.py` with this updated and corrected version:**

```python
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
```

**Run the script again:** `python etl_category.py`
