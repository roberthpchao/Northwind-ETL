## 🚀 Revised Project Plan: Northwind ETL & Data Mart

This plan covers the entire workflow from initial connection to loading a small, analytical data mart.

| Phase | Goal | Tools | Key Steps | Output |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 1: Environment & Connection** | Establish stable, isolated connection and project structure. | VS Code, Python (`venv_nw`), SSMS | Activate `venv_nw`, install libraries, configure connection strings, confirm driver compatibility. | Stable `pyodbc` connection to `localhost\SQLEXPRESS01`. |
| **Phase 2: Data Mart Schema Setup** | Design and create the target Data Mart tables for analysis. | SSMS (Manual Creation) | Create `Northwind_Reporting_DB`. Define staging, dimension (`Dim`), and fact (`Fact`) tables. | SQL scripts for Staging, `Dim_Category`, and `Fact_OrderMetrics`.  |
| **Phase 3: ETL Pipeline (Category Dimension)** | Implement the Extract, Transform, Load process for the smallest table first. | Python, `pyodbc`, Pandas | **E:** Read `NORTHWND.Categories`. **T:** Clean, stage, and generate surrogate key (if needed). **L:** Load into `Dim_Category`. | `etl_category.py` script. Verified `Dim_Category` table. |
| **Phase 4: ETL Pipeline (Product Dimension)** | Scale the ETL process to a slightly more complex table. | Python, `pyodbc`, Pandas | **E:** Read `NORTHWND.Products`. **T:** Handle foreign keys, clean text/numerical fields. **L:** Load into `Dim_Product`. | `etl_product.py` script. Verified `Dim_Product` table. |
| **Phase 5: ETL Pipeline (Order Fact)** | Implement the core analytical load, linking all dimensions. | Python, `pyodbc`, Pandas | **E:** Read `NORTHWND.Orders` and `Order Details`. **T:** Join tables, perform calculations (e.g., total price), look up dimension keys (`CategoryKey`, `ProductKey`). **L:** Load into `Fact_OrderMetrics`. | `etl_orders.py` script. Verified `Fact_OrderMetrics` table. |
| **Phase 6: Reporting & Version Control** | Finalize the data warehouse and record project success. | SSMS, Git, GitHub | Run basic analytical queries on the Fact table. Initialize Git repository, commit, and push project code. | Functional Data Mart. Updated GitHub repository. |

-----

## 🛠️ Detailed Implementation Steps

### Phase 1: Environment & Connection (Confirmation)

1.  **Create the Virtual Environment** In VS Code terminal: 'python -m venv venv_nw'.
2.  **Activate Environment:** In VS Code terminal: `.\venv_nw\Scripts\activate` (Windows).
3.  **Install Libraries:** If not already done: `pip install pyodbc pandas`.
4.  **Connection File:** Ensure `config.py` is correct (using `ODBC Driver 17 for SQL Server`).
5.  **Test Connection:** Run `python connection_test.py` to confirm stability.

### Phase 2: Data Mart Schema Setup (SSMS)

This step creates the foundational tables for your analytical reporting.

1.  **Create Reporting DB:** (If you didn't already): Create `Northwind_Reporting_DB` in SSMS.

2.  **Create Dimension Table (Category):** This table holds stable, descriptive data. We use an identity column for the **surrogate key** (`CategoryKey`).

    ```sql
    -- Target: Northwind_Reporting_DB
    CREATE TABLE Dim_Category (
        CategoryKey INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
        Source_CategoryID INT NOT NULL UNIQUE,     -- Source System Key
        CategoryName NVARCHAR(15) NOT NULL,
        CategoryDescription NTEXT
    );
    ```

3.  **Create Product Dimension Table:**

    ```sql
    -- Target: Northwind_Reporting_DB
    CREATE TABLE Dim_Product (
        ProductKey INT IDENTITY(1,1) PRIMARY KEY,
        Source_ProductID INT NOT NULL UNIQUE,
        ProductName NVARCHAR(40) NOT NULL,
        Source_CategoryID INT, -- Natural Key for Category (will be updated by ETL process)
        UnitPrice MONEY,
        UnitsInStock SMALLINT
        -- Add other relevant product fields as needed
    );
    ```

4.  **Create Fact Table (Order Metrics):** This table holds quantitative data and foreign keys (the keys link it back to the Dimensions).

    ```sql
    -- Target: Northwind_Reporting_DB
    CREATE TABLE Fact_OrderMetrics (
        OrderFactKey BIGINT IDENTITY(1,1) PRIMARY KEY,
        Source_OrderID INT NOT NULL,
        Source_ProductID INT NOT NULL,
        -- Foreign Keys linking to Dimensions
        ProductKey INT FOREIGN KEY REFERENCES Dim_Product(ProductKey),
        OrderDate DATE,
        RequiredDate DATE,
        ShippedDate DATE,
        -- Metrics
        Quantity SMALLINT NOT NULL,
        Discount REAL NOT NULL,
        ExtendedPrice MONEY NOT NULL -- Calculated metric: Quantity * UnitPrice * (1 - Discount)
    );
    ```

### Phase 3: ETL Pipeline (Category)

The goal is to load `Dim_Category` from `NORTHWND.Categories` using the **merge** or **upsert** pattern (or simple truncate/load for now).

1.  **Script:** Create `etl_category.py`.
2.  **E/T/L Logic:**
      * **Extract:** Read `CategoryID`, `CategoryName`, `Description` from source.
      * **Transform:** Rename `CategoryID` to `Source_CategoryID`.
      * **Load:** Truncate `Dim_Category` and insert the transformed data.

### Phase 4: ETL Pipeline (Product)

1.  **Script:** Create `etl_product.py`.
2.  **E/T/L Logic:**
      * **Extract:** Read `ProductID`, `ProductName`, `CategoryID`, `UnitPrice`, `UnitsInStock` from source.
      * **Transform:** Rename `ProductID` to `Source_ProductID`.
      * **Load:** Truncate `Dim_Product` and insert data.

### Phase 5: ETL Pipeline (Fact Table)

This is the most complex step as it requires a **lookup** to connect the data.

1.  **Script:** Create `etl_orders.py`.
2.  **E/T Logic:**
      * **Extract:** Read data from `NORTHWND.Orders` and `NORTHWND.Order Details` (using a SQL JOIN).
      * **Calculate:** Compute `ExtendedPrice`.
      * **Lookup:** After loading `Dim_Product`, you must fetch the mapping between `Source_ProductID` (from source) and `ProductKey` (the new surrogate key). This mapping is then used to populate the `ProductKey` column in the `Fact_OrderMetrics` table.
      * **Load:** Insert the final data into `Fact_OrderMetrics`.
