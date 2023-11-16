import os


def execute_drop_command(handler, command):
    # Parse the command to extract the table name
    tokens = command.split()
    # Assuming the format is "Drop table [table_name];"
    if len(tokens) == 3 and tokens[0].lower() == "drop" and tokens[1].lower() == "table":
        table_name = tokens[2].lower()
        drop_table(handler, table_name)
    else:
        print("Invalid drop table command.")


def drop_table(handler, table_name):
    # Ensure the command is being run in a specific database directory, not directly under SQL_DB
    if os.path.basename(os.path.normpath(handler.db_path)) == 'SQL_DB':
        print("Drop table command can only be run within a specific database directory.")
        return

    table_path = os.path.join(handler.db_path, f"{table_name}.csv")

    # Check if the table exists
    if os.path.exists(table_path):
        try:
            os.remove(table_path)
            print(f"Table '{table_name}' has been dropped.")
        except Exception as e:
            print(f"An error occurred while dropping the table: {e}")
    else:
        print(f"Table '{table_name}' does not exist.")
