import os
import pytest
import sqlalchemy as sa
from sqlalchemy import text, create_engine

# Define the path to the SQL script
SQL_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), '../../scripts/invalid_shipping_order_numbers_reconciliation.sql')

# Set up the database engine in the data folder
@pytest.fixture(scope="module")
def db_engine():
    data_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data'))
    os.makedirs(data_folder, exist_ok=True)
    db_path = os.path.join(data_folder, 'test_database.db')
    engine = create_engine(f'sqlite:///{db_path}')
    yield engine
    engine.dispose()

# Set up the database schema
@pytest.fixture(scope="module")
def setup_database(db_engine):
    with db_engine.connect() as connection:
        statements = """
            CREATE TABLE IF NOT EXISTS shippedandcollected_aramex_cosmaline (
                order_number VARCHAR(255)
            );

            CREATE TABLE IF NOT EXISTS ecom_orders (
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
        """.split(';')
        
        for statement in statements:
            if statement.strip():
                connection.execute(text(statement))
        
        connection.execute(text("""
            INSERT INTO shippedandcollected_aramex_cosmaline (order_number) VALUES
            ('ORD001'), ('ORD002'), ('ORD003'), ('ORD004');
        """))
        
        connection.execute(text("""
            INSERT INTO ecom_orders (order_number) VALUES
            ('ORD001'), ('ORD003');
        """))

    # Ensure cleanup after test
    yield
    with db_engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS shippedandcollected_aramex_cosmaline;"))
        connection.execute(text("DROP TABLE IF EXISTS ecom_orders;"))
        connection.execute(text("DROP TABLE IF EXISTS reconciliation_results;"))

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def execute_sql_script(engine, script_path):
    sql_script = read_sql_file(script_path)
    statements = sql_script.split(';')
    with engine.connect() as connection:
        for statement in statements:
            if statement.strip():
                connection.execute(text(statement))

def test_execute_reconciliation_script(db_engine, setup_database):
    execute_sql_script(db_engine, SQL_SCRIPT_PATH)

    with db_engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM reconciliation_results")).fetchall()
        
        for row in result:
            print(f"reference_id: {row['reference_id']}")
            print(f"reconciliation_status: {row['reconciliation_status']}")
            print(f"recon_report: {row['recon_report']}")

        assert len(result) == 2
        assert result[0]['reference_id'] == 'ORD002'
        assert result[0]['reconciliation_status'] == 'Mismatch'
        assert result[0]['recon_report'] == 'Mismatch due to non-existent record'
        assert result[1]['reference_id'] == 'ORD004'
        assert result[1]['reconciliation_status'] == 'Mismatch'
        assert result[1]['recon_report'] == 'Mismatch due to non-existent record'