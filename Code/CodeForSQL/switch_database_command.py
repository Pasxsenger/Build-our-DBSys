import os


def execute_switch_command(handler, command):
    # Parse the command to extract the database name
    tokens = command.split()
    # The format is "Switch to database [database_name];"
    if len(tokens) == 4 and tokens[0].lower() == "switch" and tokens[1].lower() == "to" and tokens[2].lower() == "database":
        db_name = tokens[3].lower()
        switch_database(handler, db_name)
    else:
        print("Invalid switch command.")


def switch_database(handler, db_name):
    # Check if we're already at the SQL_DB level
    if os.path.basename(handler.db_path) == 'SQL_DB':
        new_db_path = os.path.join(handler.db_path, db_name)
        # print(new_db_path)
    else:
        # Go up one level to the SQL_DB directory
        new_db_path = os.path.join(os.path.dirname(handler.db_path), db_name)
        # print(new_db_path)

    # Check if the desired database directory exists
    if os.path.exists(new_db_path):
        handler.db_path = new_db_path  # Update the handler's database path
        print(f"Switched to database '{db_name}'.")
    else:
        print(f"Database '{db_name}' does not exist. Please create it first.")
