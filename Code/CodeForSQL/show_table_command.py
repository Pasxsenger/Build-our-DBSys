import os


def execute_tables_command(handler, command):
    # The command is simply "tables;"
    if command.lower() == "tables":
        show_tables(handler)
    else:
        print("Invalid command for showing tables.")


def show_tables(handler):
    # Ensure the command is not being run directly under SQL_DB
    if os.path.basename(os.path.normpath(handler.db_path)) == 'SQL_DB':
        print("Show tables command can only be run within a specific database directory.")
        return

    # Get the name of the current database
    database_name = os.path.basename(os.path.normpath(handler.db_path))

    try:
        # List all CSV files in the current database directory
        table_files = [file for file in os.listdir(handler.db_path) if file.endswith('.csv')]
        print_tables(table_files, database_name)

        # List all CSV files in the [database_name]_running directory
        running_path = os.path.join(handler.db_path, f"{database_name}_running")
        if os.path.exists(running_path):
            running_table_files = [file for file in os.listdir(running_path) if file.endswith('.csv')]
            print_tables(running_table_files, f"{database_name}_running")
        else:
            print(f"No '{database_name}_running' directory found.")

    except Exception as e:
        print(f"An error occurred while listing the tables: {e}")


def print_tables(tables, directory_name):
    # Print the header with the directory name
    header = f"| Tables_in_{directory_name}".ljust(30) + "|"
    print("+------------------------------+")
    print(header)
    print("+------------------------------+")

    # Print each table name (without the '.csv' extension)
    for table in tables:
        table_name = table.rsplit('.', 1)[0]  # Remove the '.csv' extension
        print(f"| {table_name.ljust(28)} |")

    # Print the footer
    print("+------------------------------+")
    print(f"{len(tables)} rows in set\n")
