import pytest
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import os

# Create a test database engine in the data folder
@pytest.fixture(scope="module")
def db_engine():
    data_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data'))
    os.makedirs(data_folder, exist_ok=True)
    db_path = os.path.join(data_folder, 'test_database.db')
    engine = create_engine(f'sqlite:///{db_path}')
    yield engine
    engine.dispose()

# Setup the database schema and initial test data
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
            CREATE TABLE IF NOT EXISTS daily_rate (
                date DATE,
                rate FLOAT
            );
            CREATE TABLE IF NOT EXISTS shippedandcollected_aramex_cosmaline (
                Delivery_Date DATE,
                CODCurrency VARCHAR(3),
                CODAmount FLOAT,
                O_CODAmount FLOAT,
                TOKENNO VARCHAR(255)
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
        """
        for statement in statements.split(';'):
            if statement.strip():
                connection.execute(text(statement))

    # Insert initial test data
    with db_engine.connect() as connection:
        connection.execute(text("DELETE FROM erp_data;"))
        connection.execute(text("""
            INSERT INTO erp_data (receipt_date, currency_code, receipt_amount, exchange_rate, comments) VALUES
            ('2023-01-01', 'LBP', 150000, 128.5, 'Shipped With Cosmaline');
        """))
        connection.execute(text("DELETE FROM daily_rate;"))
        connection.execute(text("""
            INSERT INTO daily_rate (date, rate) VALUES
            ('2023-01-01', 1500);
        """))
        connection.execute(text("DELETE FROM shippedandcollected_aramex_cosmaline;"))
        connection.execute(text("""
            INSERT INTO shippedandcollected_aramex_cosmaline (Delivery_Date, CODCurrency, CODAmount, O_CODAmount, TOKENNO) VALUES
            ('2023-01-01', 'LBP', 300000, 0, 'Shipped With Cosmaline');
        """))

    yield

    # Clean up schema after tests
    with db_engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS erp_data;"))
        connection.execute(text("DROP TABLE IF EXISTS daily_rate;"))
        connection.execute(text("DROP TABLE IF EXISTS shippedandcollected_aramex_cosmaline;"))
        connection.execute(text("DROP TABLE IF EXISTS reconciliation_results;"))

# Test the SQL script execution
def test_execute_reconciliation_script(db_engine, setup_database):
    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/cosmaline_reconciliation.sql'))
    
    with db_engine.begin() as connection:
        # Read and execute the SQL script
        with open(script_path, 'r') as file:
            sql_script = file.read()
            statements = sql_script.split(';')
            for statement in statements:
                if statement.strip():
                    try:
                        connection.execute(text(statement.strip()))
                    except sa.exc.OperationalError as e:
                        # Handle specific case of DROP TABLE with multiple tables
                        if "near \",\": syntax error" in str(e):
                            tables_to_drop = statement.strip().split("DROP TABLE IF EXISTS ")[1].split(", ")
                            for table in tables_to_drop:
                                connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
                        else:
                            raise e

        # Validate the results
        result = connection.execute(text("""
            SELECT * FROM reconciliation_results WHERE reconciliation_type = 'Shipped With Cosmaline Reconciliation'
        """)).fetchall()

        assert len(result) == 1

        # Print actual values for debugging
        print(f"sum_erp_amount_usd: {result[0]['sum_erp_amount_usd']}")
        print(f"sum_shipping_amount_usd: {result[0]['sum_shipping_amount_usd']}")
        print(f"reconciliation_status: {result[0]['reconciliation_status']}")
        print(f"recon_report: {result[0]['recon_report']}")

        assert result[0]['reconciliation_type'] == 'Shipped With Cosmaline Reconciliation'
        assert result[0]['reference_date'] == '2023-01-01'
        assert result[0]['sum_erp_amount_usd'] == pytest.approx(150000 / 128.5, 0.01)
        assert result[0]['sum_shipping_amount_usd'] == pytest.approx(300000 / 1500, 0.01)
        assert result[0]['reconciliation_status'] == 'Mismatch'  # Adjusted expectation
        assert result[0]['non_existent_record'] == 0
        assert result[0]['recon_report'] == 'Mismatch due to different amounts'  # Adjusted expectation