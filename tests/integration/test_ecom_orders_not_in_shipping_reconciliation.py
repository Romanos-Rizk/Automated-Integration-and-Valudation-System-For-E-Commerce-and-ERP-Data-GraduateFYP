import pytest
import os
from sqlalchemy import create_engine, text

# Define the path to the SQL script
SQL_SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/ecom_orders_not_in_shipping_reconciliation.sql'))

# Function to execute SQL script from a file
def execute_sql_script(engine, script_path):
    with engine.connect() as connection:
        with open(script_path, 'r') as file:
            sql_script = file.read()
            sql_script = sql_script.replace("COMMIT;", "")  # Remove COMMIT; for SQLite
            statements = sql_script.split(';')
            for statement in statements:
                if statement.strip():
                    connection.execute(text(statement.strip()))

# Fixture to create a database engine in the data folder
@pytest.fixture(scope="module")
def db_engine():
    data_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data'))
    os.makedirs(data_folder, exist_ok=True)
    db_path = os.path.join(data_folder, 'test_database.db')
    engine = create_engine(f'sqlite:///{db_path}', echo=True)
    yield engine
    engine.dispose()

# Fixture to setup the database schema
@pytest.fixture(scope="module")
def setup_database(db_engine):
    with db_engine.connect() as connection:
        statements = """
            CREATE TABLE IF NOT EXISTS ecom_orders (
                order_number VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS shippedandcollected_aramex_cosmaline (
                order_number VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS reconciliation_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reconciliation_type VARCHAR(255),
                reference_id VARCHAR(255),
                reconciliation_status VARCHAR(50),
                non_existent_record INTEGER,
                recon_report VARCHAR(255)
            );
        """.split(";")
        for statement in statements:
            if statement.strip():
                connection.execute(text(statement.strip()))

# Fixture to seed the initial data
@pytest.fixture(scope="function")
def seed_data(db_engine):
    with db_engine.connect() as connection:
        statements = """
            DELETE FROM ecom_orders;
            DELETE FROM shippedandcollected_aramex_cosmaline;
            DELETE FROM reconciliation_results;

            INSERT INTO ecom_orders (order_number) VALUES
            ('ORD001'), ('ORD002'), ('ORD003');

            INSERT INTO shippedandcollected_aramex_cosmaline (order_number) VALUES
            ('ORD001');
        """.split(";")
        for statement in statements:
            if statement.strip():
                connection.execute(text(statement.strip()))

    # Ensure cleanup after test
    yield
    with db_engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS ecom_orders;"))
        connection.execute(text("DROP TABLE IF EXISTS shippedandcollected_aramex_cosmaline;"))
        connection.execute(text("DROP TABLE IF EXISTS reconciliation_results;"))

# Test case to execute the reconciliation script and validate the results
def test_execute_reconciliation_script(db_engine, setup_database, seed_data):
    script_path = SQL_SCRIPT_PATH
    
    with db_engine.connect() as connection:
        execute_sql_script(connection, script_path)
        
        result = connection.execute(text("""
            SELECT 
                reconciliation_type, 
                reference_id, 
                reconciliation_status, 
                non_existent_record, 
                recon_report 
            FROM reconciliation_results
            WHERE reconciliation_type = 'ECOM Orders not in Shipping'
        """)).fetchall()

        # Print intermediate results for debugging
        for row in result:
            print(f"reference_id: {row['reference_id']}")
            print(f"reconciliation_status: {row['reconciliation_status']}")
            print(f"non_existent_record: {row['non_existent_record']}")
            print(f"recon_report: {row['recon_report']}")

        assert len(result) == 2
        assert result[0]['reference_id'] == 'ORD002'
        assert result[0]['reconciliation_status'] == 'Mismatch'
        assert result[0]['non_existent_record'] == 1
        assert result[0]['recon_report'] == 'Mismatch due to non-existent record'
        assert result[1]['reference_id'] == 'ORD003'
        assert result[1]['reconciliation_status'] == 'Mismatch'
        assert result[1]['non_existent_record'] == 1
        assert result[1]['recon_report'] == 'Mismatch due to non-existent record'