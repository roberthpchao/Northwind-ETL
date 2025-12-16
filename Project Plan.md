## 🗺️ Project Plan: Northwind ETL

| Phase | Goal | Tools | Key Steps |
| :--- | :--- | :--- | :--- |
| **Phase 1: Setup & Connection** | Establish a stable, reliable connection from Python to your SQL Server. | VS Code, Python, `pyodbc`, SSMS | Install libraries, confirm driver, test connection. |
| **Phase 2: Database Preparation** | Create the target/reporting database and the first staging table structure. | SSMS, Python | Manually create `Northwind_Reporting_DB` (if needed), define staging table. |
| **Phase 3: ETL - Extract & Load** | Extract data from `NORTHWND` and load it into a Staging table in `Northwind_Reporting_DB`. | Python, `pyodbc` | Write the Python ETL script for a simple table (e.g., `Categories`). |
| **Phase 4: Transformation & Analysis**| Transform staged data and load it into a final fact/dimension table. | Python, SQL | Implement a simple transformation, load into a final schema. |
| **Phase 5: Version Control** | Record success and track progress on GitHub. | Git, VS Code | Initialize Git, commit code, push to GitHub. |

-----

## 🛠️ Step-by-Step Implementation

### **Phase 1: Setup & Connection**

This is the most critical step to avoid server crash/connection loss.

#### **Step 1: Install Python Libraries**

Open your VS Code terminal in the `Northwind_ETL` project folder and install the necessary libraries.

```bash
pip install pyodbc pandas
```

  * `pyodbc`: The primary library for connecting to ODBC databases (like MS SQL Server).
  * `pandas`: Excellent for handling data extraction and transformation.

#### **Step 2: Define Connection Parameters**

Create a new file named `config.py` in your project folder and add your connection details. **We will use the recommended driver for modern SQL Server installations.**

```python
# config.py

# Your SQL Server details
SERVER_NAME = 'localhost\\SQLEXPRESS01'
SOURCE_DATABASE = 'NORTHWND'
REPORTING_DATABASE = 'Northwind_Reporting_DB'

# Connection string for Windows Authentication
# The 'ODBC Driver 17 for SQL Server' is highly recommended for SQL Server 2022/2025
# Ensure this driver is installed on your system (it usually is with SSMS/SQL Server installs)
CONNECTION_STRING = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={SERVER_NAME};'
    f'DATABASE={SOURCE_DATABASE};'
    f'Trusted_Connection=yes;'
)

# Reporting DB connection string (use this for writing data)
REPORTING_CONNECTION_STRING = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={SERVER_NAME};'
    f'DATABASE={REPORTING_DATABASE};'
    f'Trusted_Connection=yes;'
)
```

#### **Step 3: Test the Connection (Crucial)**

Create a file named `connection_test.py` to confirm the connection is stable before doing any heavy operations.

```python
# connection_test.py
import pyodbc
from config import CONNECTION_STRING, SERVER_NAME, SOURCE_DATABASE

print(f"Attempting to connect to: {SERVER_NAME} / {SOURCE_DATABASE}...")

try:
    # Use pyodbc.connect with the defined connection string
    cnxn = pyodbc.connect(CONNECTION_STRING)
    cursor = cnxn.cursor()

    # Execute a simple query to confirm data can be fetched
    cursor.execute("SELECT COUNT(*) FROM Customers")
    count = cursor.fetchone()[0]

    print("✅ Connection successful!")
    print(f"Total rows in Customers table: {count}")

    # Close the connection immediately after the test
    cursor.close()
    cnxn.close()
    print("Connection closed.")

except pyodbc.Error as ex:
    # Print the specific ODBC error
    sqlstate = ex.args[0]
    print(f"❌ Connection failed! ODBC Error: {sqlstate}")
    print("Possible causes: Incorrect Server Name, missing or wrong ODBC Driver, or firewall.")

```

*Run this script: `python connection_test.py`.* **This must succeed before moving on.** If it fails, you'll need to confirm the exact name of your ODBC driver (e.g., `ODBC Driver 13 for SQL Server` if you have an older version) or verify your server name.

### **Phase 2: Database Preparation**

#### **Step 4: Create Reporting Database (SSMS)**

To be safe and avoid potential Python/server conflicts, use **SQL Server Management Studio (SSMS)** to ensure the destination database exists.

1.  Connect to your `localhost\SQLEXPRESS01` server.
2.  In the Object Explorer, right-click **Databases** and select **New Database...**
3.  Name it `Northwind_Reporting_DB`.

#### **Step 5: Define Staging Table (SSMS)**

For our first ETL, let's work with the `Categories` table. We'll create a staging table in the new reporting database.

1.  In SSMS, open a new query window targeting the `Northwind_Reporting_DB`.
2.  Run the following SQL to create a simple Staging table:

<!-- end list -->

```sql
-- In Northwind_Reporting_DB
CREATE TABLE Staging_Categories (
    Source_CategoryID INT NOT NULL,
    CategoryName NVARCHAR(15) NOT NULL,
    CategoryDescription NTEXT
    -- No Primary Key, as this is just a temporary holding area
);
```

### **Phase 3: ETL - Extract & Load**

#### **Step 6: Write the ETL Script**

Create a file named `etl_categories.py`. This script will extract data from the source, clean/stage it, and load it into the reporting database.

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

*Run this script: `python etl_categories.py`.*

#### **Step 7: Verify Data (SSMS)**

Go back to SSMS, target `Northwind_Reporting_DB`, and run:

```sql
SELECT * FROM Staging_Categories; --changed to dim_category
```

You should see all 8 rows from the Northwind `Categories` table successfully loaded.

### **Phase 4 & 5 (Next Steps)**

Now that your connection is stable and a basic ETL is proven:

  * **Phase 4 (Transformation):** You would typically create a final table (e.g., `Dim_Category`) and write a **transformation step** in Python/SQL to move the data from `Staging_Categories` into the final dimension table, often applying data cleaning or business logic.
  * **Phase 5 (Version Control):** Initialize Git in your project and make your first commit:

<!-- end list -->

```bash
git init
git add .
git commit -m "Initial setup: config, connection test, and Categories ETL scripts."
```

-----

## 🙋 What would you like to do next?

1.  **Start running the Python scripts** now that you have the plan?
2.  **Focus on the transformation logic** (Phase 4) for the `Categories` table?
3.  **Troubleshoot an issue** if the `connection_test.py` script failed?