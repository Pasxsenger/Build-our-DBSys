import os
import csv


def execute_set_command(handler, command):
    # Split the command into two parts: table name and column definitions
    parts = command.split(" with columns ")
    if len(parts) != 2:
        print("Invalid set table command format.")
        return

    # Extract table name and column definitions
    table_name_part = parts[0].split()
    column_definitions_part = parts[1]

    # The format is "Set up a new table named [table_name] with columns [column_definitions]"
    if len(table_name_part) >= 7 and " ".join(table_name_part[:6]).lower() == "set up a new table named":
        table_name = table_name_part[6].strip().strip(";").lower()
        # Process column definitions
        columns = [col.strip().strip(",").lower() for col in column_definitions_part.split(",")]
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
