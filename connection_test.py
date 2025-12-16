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
