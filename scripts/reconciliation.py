import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sys
sys.path.append('/opt/airflow/scripts')  # Ensure the scripts directory is in the system path

from load import create_db_engine

from sqlalchemy.orm import sessionmaker
import sqlalchemy as sa

def execute_reconciliation_script(script_filename):
    # Create a database engine using the function from load.py
    engine = create_db_engine()
    
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Read the SQL script
        script_path = f'/opt/airflow/scripts/{script_filename}'
        with open(script_path, 'r') as file:
            sql_script = file.read()
        
        # Split the script into individual statements
        statements = sql_script.split(';')
        
        # Execute each statement individually
        for statement in statements:
            if statement.strip():
                session.execute(sa.text(statement))
        
        session.commit()
        
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    
    finally:
        session.close()

