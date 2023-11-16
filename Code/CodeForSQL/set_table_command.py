import os
import csv


def execute_set_command(handler, command):
    # Parse the command to extract table name and column definitions
    tokens = command.split()
    # Assuming the format is "Set up a new table named [table_name] with columns [column_definitions]"
    if len(tokens) >= 10 and " ".join(tokens[:6]).lower() == "set up a new table named" and tokens[8].lower() == "columns":
        table_name = tokens[6].lower()
        columns = [token.strip(",").lower() for token in tokens[9:]]
        set_table(handler, table_name, columns)
    else:
        print("Invalid set table command.")


def set_table(handler, table_name, columns):
    # Ensure the command is being run in a specific database directory, not directly under SQL_DB
    if os.path.basename(os.path.normpath(handler.db_path)) == 'SQL_DB':
        print("Set table command can only be run within a specific database directory.")
        return

    table_path = os.path.join(handler.db_path, f"{table_name}.csv")

    # Check if the table already exists
    if os.path.exists(table_path):
        print(f"Table '{table_name}' already exists.")
        return

    # Create the CSV file with column headers
    try:
        with open(table_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(columns)  # Write the column headers
        print(f"Table '{table_name}' created successfully with columns {', '.join(columns)}.")
    except Exception as e:
        print(f"An error occurred while creating the table: {e}")
