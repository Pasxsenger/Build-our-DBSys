import os
import shutil


def execute_delete_command(handler, command):
    # Parse the command to extract the database name
    tokens = command.split()
    # The format is "Delete database [database_name];"
    if len(tokens) == 3 and tokens[0].lower() == "delete" and tokens[1].lower() == "database":
        db_name = tokens[2].lower()
        delete_database(handler, db_name)
    else:
        print("Invalid delete command.")


def delete_database(handler, db_name):
    # Determine the base SQL_DB path
    base_path = handler.db_path
    # If the current path is a sub-directory of SQL_DB, move one level up
    if os.path.basename(base_path) != 'SQL_DB':
        base_path = os.path.dirname(base_path)

    # Define the path for the database to be deleted
    db_path = os.path.join(base_path, db_name)
    if os.path.exists(db_path):
        # Remove the directory
        try:
            shutil.rmtree(db_path)
            print(f"Database '{db_name}' has been deleted.")
        except Exception as e:
            print(f"An error occurred while deleting the database: {e}")
    else:
        print(f"Database '{db_name}' does not exist.")
