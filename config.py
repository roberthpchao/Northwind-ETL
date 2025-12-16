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