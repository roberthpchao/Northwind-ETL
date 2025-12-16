That is a critical and informative error\! You've run into a core concept of database integrity: **Foreign Key Constraints**.

The error: `Cannot truncate table 'Dim_Product' because it is being referenced by a FOREIGN KEY constraint.`

This means that during the previous attempts or manual SQL work, you must have created the `Fact_OrderMetrics` table (or another table) that contains a foreign key pointing to `Dim_Product`.

### The Problem

The `TRUNCATE TABLE` command in SQL Server is extremely fast, but it only works if no other table has a Foreign Key relationship pointing to the table being truncated. Since our plan involves creating the `Fact_OrderMetrics` table, which *should* link to `Dim_Product` via `ProductKey`, your system is correctly stopping you from deleting data in `Dim_Product` that the fact table might need.

### The Solution: Two Options

Since we are still in the development and initial loading phase (where we expect to reload data often), the cleanest solution is to temporarily disable the constraint that is causing the lock.

#### **Option A: The ETL-Friendly Fix (Recommended)**

We need to tell the server to temporarily ignore the constraints on the fact table when we load its associated dimension table.

1.  **Modify the Load Function:** In your `etl_product.py` script, we will wrap the `TRUNCATE` command with logic to disable and re-enable the constraints on the referencing table (`Fact_OrderMetrics`).

    We must assume the referencing table is `Fact_OrderMetrics` as per the plan.

    **Update the `load_dimension_table` function in your `etl_product.py` file with the following code:**

    ```python
    # ... (Keep Extract and Transform functions the same)

    # --- 3. LOAD (MODIFIED) ---
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

            # --- CRITICAL FIX: Temporarily Disable Constraints on Referencing Table ---
            print(f"   Disabling constraints on {REFERENCING_TABLE}...")
            # This allows TRUNCATE on Dim_Product to succeed.
            cursor.execute(f"ALTER TABLE {REFERENCING_TABLE} NOCHECK CONSTRAINT ALL")

            # Truncate and Reload Strategy
            cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE}")
            print(f"   Table {TARGET_TABLE} truncated.")
            
            # Prepare the INSERT statement (8 columns)
            insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (
                Source_ProductID, ProductName, Source_SupplierID, Source_CategoryID, 
                QuantityPerUnit, UnitPrice, UnitsInStock, Discontinued
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """

            # Prepare data for fast insertion
            data_to_insert = [tuple(row) for row in df.values]
            
            cursor.executemany(insert_sql, data_to_insert)
            
            # --- CRITICAL FIX: Re-enable Constraints and Check Data Integrity ---
            print(f"   Re-enabling constraints on {REFERENCING_TABLE}...")
            cursor.execute(f"ALTER TABLE {REFERENCING_TABLE} CHECK CONSTRAINT ALL")
            
            # Check constraints just to ensure no bad data slipped in (important for production)
            cursor.execute(f"DBCC CHECKCONSTRAINTS WITH ALL_CONSTRAINTS") # This runs silently unless issues found
            
            cnxn.commit()
            cursor.close()
            cnxn.close()

            print(f"   ✅ Successfully loaded {len(df)} rows into {TARGET_TABLE}.")

        except pyodbc.Error as ex:
            print(f"Loading failed: {ex}")
            cnxn.rollback()

    # ... (Keep the main execution block the same)
    ```

#### **Option B: Manual Cleanup (Alternative)**

If you are certain you have not yet created the `Fact_OrderMetrics` table, this means you must have created a different table that references `Dim_Product`. The easiest way to fix this manually is to **drop the referencing table** until we are ready to build the fact load in Phase 5.

1.  **In SSMS, drop the Fact Table:**
    ```sql
    -- Target: Northwind_Reporting_DB
    -- !!! ONLY RUN THIS IF YOU HAVEN'T LOADED ANY IMPORTANT DATA INTO IT YET !!!
    DROP TABLE Fact_OrderMetrics; 

    -- If you created Dim_Category before, check if that also needs dropping (it shouldn't reference Dim_Product)
    ```
