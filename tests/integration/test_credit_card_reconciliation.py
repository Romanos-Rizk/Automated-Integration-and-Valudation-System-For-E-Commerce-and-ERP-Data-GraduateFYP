import pytest
from sqlalchemy import create_engine, text
import os

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
            CREATE TABLE IF NOT EXISTS erp_data (
                receipt_date DATE,
                currency_code VARCHAR(3),
                receipt_amount FLOAT,
                exchange_rate FLOAT,
                comments VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS credit_card (
                value_date DATE,
                amount FLOAT
            );
            CREATE TABLE IF NOT EXISTS reconciliation_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reconciliation_type VARCHAR(255),
                reference_date DATE,
                sum_erp_amount_usd FLOAT,
                sum_shipping_amount_usd FLOAT,
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
            DELETE FROM erp_data;
            DELETE FROM credit_card;
            DELETE FROM reconciliation_results;

            INSERT INTO erp_data (receipt_date, currency_code, receipt_amount, exchange_rate, comments) VALUES
            ('2023-01-01', 'LBP', 15833650, 95000, 'Cybersource'),
            ('2023-01-01', 'USD', 1000, 95000, 'Cybersource');
            
            INSERT INTO credit_card (value_date, amount) VALUES
            ('2023-01-01', 1166.67);
        """.split(";")
        for statement in statements:
            if statement.strip():
                connection.execute(text(statement.strip()))

    # Ensure cleanup after test
    yield
    with db_engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS erp_data;"))
        connection.execute(text("DROP TABLE IF EXISTS credit_card;"))
        connection.execute(text("DROP TABLE IF EXISTS reconciliation_results;"))

# Test case to execute the reconciliation script and validate the results
def test_execute_reconciliation_script(db_engine, setup_database, seed_data):
    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/credit_card_reconciliaiton.sql'))
    
    # Adjust the script content to split DROP TABLE statements
    with open(script_path, 'r') as file:
        sql_script = file.read()
        sql_script = sql_script.replace(
            "DROP TABLE IF EXISTS erp_cybersource_sum, credit_card_sum;",
            "DROP TABLE IF EXISTS erp_cybersource_sum; DROP TABLE IF EXISTS credit_card_sum;"
        )
        sql_script = sql_script.replace("COMMIT;", "")  # Remove COMMIT; for SQLite
    
    with db_engine.connect() as connection:
        statements = sql_script.split(';')
        for statement in statements:
            if statement.strip():
                connection.execute(text(statement.strip()))

    with db_engine.connect() as connection:
        result = connection.execute(text("""
            SELECT 
                reconciliation_type, 
                reference_date, 
                sum_erp_amount_usd, 
                sum_shipping_amount_usd, 
                reconciliation_status, 
                recon_report 
            FROM reconciliation_results
            WHERE reconciliation_type = 'Cybersource Reconciliation'
        """)).fetchall()

        assert len(result) == 1
        assert result[0]['reconciliation_type'] == 'Cybersource Reconciliation'
        assert result[0]['reference_date'] == '2023-01-01'
        assert abs(result[0]['sum_erp_amount_usd'] - 1166.67) < 0.1
        assert abs(result[0]['sum_shipping_amount_usd'] - 1166.67) < 0.1 
        assert result[0]['reconciliation_status'] == 'Match'
        assert result[0]['recon_report'] == 'Match'