### 🔍 Re-evaluating the Foreign Key Error

If the error persists *even after* adding the `ALTER TABLE NOCHECK` logic, there are only two logical possibilities:

1.  **The Referencing Table is Named Differently:** The constraint is being enforced by a table *other than* `Fact_OrderMetrics`, or perhaps the constraint itself is named differently, causing the `ALTER TABLE` command to fail or not apply correctly.
2.  **The `ALTER TABLE NOCHECK` Command Failed:** The Python script failed to execute the `ALTER TABLE Fact_OrderMetrics NOCHECK CONSTRAINT ALL` command because of a typo in the table name or a lack of specific permissions, causing the script to jump directly to the `TRUNCATE TABLE` which then failed because the constraints were still active.

### 📝 Step-by-Step Diagnostic and Fix

We need to confirm the exact nature of the constraint.

#### **Step 1: Diagnose the Constraint in SSMS (Crucial)**

Execute the following SQL query in **SSMS** targeting the **`Northwind_Reporting_DB`**. This will tell us definitively *which table* is referencing `Dim_Product` and the name of the Foreign Key constraint.

```sql
SELECT 
    referencing_table = OBJECT_NAME(fk.parent_object_id),
    constraint_name = fk.name
FROM 
    sys.foreign_keys fk
INNER JOIN 
    sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
WHERE 
    OBJECT_NAME(fk.referenced_object_id) = 'Dim_Product';
```

**Expected Result:** You should see one row with `referencing_table` as `Fact_OrderMetrics` and a constraint name like `FK__Fact_Orde__Produ__...`

**Action:** **Please paste the result of this query here.** This will confirm the table name and the constraint name.

#### **Step 2: Modify Python Script for Robustness (Hypothesis Fix)**

Based on the diagnosis, we can make the script more robust by using a standard `DELETE` command instead of `TRUNCATE`. While `TRUNCATE` is faster, `DELETE` does not have the same restrictions regarding Foreign Keys (it only fails if the foreign key violation occurs *during* the delete operation, not before). Since we expect the fact table to be empty at this stage, `DELETE` is a safe alternative.

**Update the Load function in `etl_product.py` as follows (Option C):**

```python
# --- 3. LOAD (Option C: Using DELETE instead of TRUNCATE) ---
def load_dimension_table(df):
    if df.empty:
        print("   No data to load. Skipping Load phase.")
        return

    TARGET_TABLE = "Dim_Product" 

    print(f"3. Loading data into Northwind_Reporting_DB.{TARGET_TABLE}...")
    try:
        cnxn = pyodbc.connect(REPORTING_CONNECTION_STRING)
        cursor = cnxn.cursor()

        # Simple Load Strategy: DELETE and Reload
        # DELETE bypasses the Foreign Key TRUNCATE restriction.
        cursor.execute(f"DELETE FROM {TARGET_TABLE}")
        print(f"   Table {TARGET_TABLE} cleared using DELETE.")
        
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
        
        cnxn.commit()
        cursor.close()
        cnxn.close()

        print(f"   ✅ Successfully loaded {len(df)} rows into {TARGET_TABLE}.")

    except pyodbc.Error as ex:
        print(f"Loading failed: {ex}")
        cnxn.rollback()
# ... (Keep the rest of the file the same)
```

### 🎯 Next Step:

Please proceed with **Step 1 (the SSMS diagnostic query)** and share the result, or try **Step 2 (the Python script modification using `DELETE`)** and let me know if it solves the load issue.

### diagnose
referencing_table	constraint_name
Fact_OrderMetrics	FK__Fact_Orde__Produ__628FA481
I haven't try step 2 python delete yet.

The diagnosis confirms two crucial facts:

1.  **Referencing Table:** The table causing the lock is indeed **`Fact_OrderMetrics`**.
2.  **Constraint Name:** The specific constraint is named **`FK__Fact_Orde__Produ__628FA481`**.

Since the `ALTER TABLE NOCHECK` command failed in Python (`Option A`), it means that the Python connection either didn't have the necessary permissions to disable the constraint, or (more likely in the ETL context) the initial `ALTER TABLE Fact_OrderMetrics NOCHECK CONSTRAINT ALL` statement somehow failed to execute successfully due to connection timing or transaction issues.

### 🎯 Recommended Fix: Use `DELETE` (Option C)

The simplest and most reliable solution during the development and testing phase of an ETL pipeline is to avoid the tricky `TRUNCATE`/`ALTER CONSTRAINT` dance entirely and use the `DELETE` statement.

The `DELETE FROM Dim_Product` command will clear all rows without violating the foreign key integrity, as the foreign key check only happens when you try to insert/update a row, not when you delete the entire contents.

### 📝 Step-by-Step Action

#### **Step 1: Implement the `DELETE` Strategy in Python**

Please implement **Option C** by updating the `load_dimension_table` function in your `etl_product.py` file to use `DELETE` instead of `TRUNCATE` and removing the constraint logic.

```python
# etl_product.py - Update the LOAD function only

# --- 3. LOAD (Option C: Using DELETE instead of TRUNCATE) ---
def load_dimension_table(df):
    if df.empty:
        print("   No data to load. Skipping Load phase.")
        return

    TARGET_TABLE = "Dim_Product" 

    print(f"3. Loading data into Northwind_Reporting_DB.{TARGET_TABLE}...")
    try:
        cnxn = pyodbc.connect(REPORTING_CONNECTION_STRING)
        cursor = cnxn.cursor()

        # Simple Load Strategy: DELETE and Reload
        # DELETE bypasses the Foreign Key TRUNCATE restriction.
        cursor.execute(f"DELETE FROM {TARGET_TABLE}")
        print(f"   Table {TARGET_TABLE} cleared using DELETE.")
        
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
```

#### **Step 2: Run the Script**

Run the updated script from your terminal:

```bash
(venv_nw) python etl_product.py
```

---

###
Loading failed: ('42S22', "[42S22] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Invalid column name 'Source_SupplierID'. (207) (SQLExecDirectW); [42S22] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Invalid column name 'QuantityPerUnit'. (207); [42S22] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Invalid column name 'Discontinued'. (207); [42S22] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Statement(s) could not be prepared. (8180)")

The error message `Invalid column name 'Source_SupplierID'` (and similar for `QuantityPerUnit` and `Discontinued`) means the issue is *not* with the `DELETE` command this time, but with the **`INSERT` statement**.

### 🔍 The Cause: Mismatch in Column Naming

The `INSERT` statement is failing because the Python script is trying to use column names that **do not exist** in the `Dim_Product` table definition we created in SSMS.

This is happening because:

1.  **Extraction and Transformation:** The Python script renames the columns:
    ```python
    df.rename(columns={'SupplierID': 'Source_SupplierID', ...}, inplace=True)
    ```
2.  **Load/Insert:** The final `INSERT` SQL statement uses these renamed columns:
    ```sql
    INSERT INTO Dim_Product (Source_ProductID, ProductName, Source_SupplierID, ...) VALUES (?, ?, ?, ...)
    ```

The logical conclusion is that the column names in the **SQL table definition (`Dim_Product`)** do not exactly match the names used in the Python script.

### 📝 Step-by-Step Fix: Correct the SQL Table Schema (SSMS)

To fix this, we need to correct the `Dim_Product` table in your database to match the column names your Python script is generating. We will drop the existing table and recreate it with the precise column names used in the Python script's `INSERT`.

#### **Step 1: Drop Existing Tables (SSMS)**

Because `Dim_Product` is referenced by `Fact_OrderMetrics`, you must drop the fact table first.

Go to **SSMS** targeting `Northwind_Reporting_DB` and run the following in sequence:

```sql
-- 1. Drop the table referencing Dim_Product first
IF OBJECT_ID('Fact_OrderMetrics') IS NOT NULL
BEGIN
    DROP TABLE Fact_OrderMetrics;
END

-- 2. Then, drop the Dim_Product table
IF OBJECT_ID('Dim_Product') IS NOT NULL
BEGIN
    DROP TABLE Dim_Product;
END
```

#### **Step 2: Recreate Tables with Correct Schema (SSMS)**

Now, recreate `Dim_Product` with the column names that match what your Python script is inserting, and then recreate `Fact_OrderMetrics`.

```sql
-- Target: Northwind_Reporting_DB

-- A. Recreate Dim_Product with correct column names (matching Python's output)
CREATE TABLE Dim_Product (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    Source_ProductID INT NOT NULL UNIQUE,
    ProductName NVARCHAR(40) NOT NULL,
    -- CORRECTED column names to match Python:
    Source_SupplierID INT,       
    Source_CategoryID INT,       
    QuantityPerUnit NVARCHAR(20), -- This name was correct in the previous SQL but checking again
    UnitPrice MONEY,
    UnitsInStock SMALLINT,
    Discontinued BIT            -- This name was correct in the previous SQL but checking again
);

-- B. Recreate Fact_OrderMetrics (required for the Foreign Key constraint)
CREATE TABLE Fact_OrderMetrics (
    OrderFactKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_OrderID INT NOT NULL,
    Source_ProductID INT NOT NULL,
    -- Foreign Key references the new Dim_Product
    ProductKey INT FOREIGN KEY REFERENCES Dim_Product(ProductKey), 
    OrderDate DATE,
    RequiredDate DATE,
    ShippedDate DATE,
    Quantity SMALLINT NOT NULL,
    Discount REAL NOT NULL,
    ExtendedPrice MONEY NOT NULL 
);
```

#### **Step 3: Re-Run the Python Script**

Since you've corrected the schema mismatch, your current `etl_product.py` script (using the `DELETE` strategy) should now run successfully.

Run the script from your terminal again:

```bash
(venv_nw) python etl_product.py
```
