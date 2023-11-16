import os


def execute_databases_command(handler, command):
    # Assuming the command is simply "databases;"
    if command.lower() == "databases":
        show_databases(handler)
    else:
        print("Invalid command for showing databases.")


def show_databases(handler):
    # Determine the base SQL_DB path
    base_path = handler.db_path
    # If the current path is a sub-directory of SQL_DB, move one level up
    if os.path.basename(base_path) != 'SQL_DB':
        base_path = os.path.dirname(base_path)

    try:
        # List all directories in the base_path
        database_dirs = [name for name in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, name))]
        print_databases(database_dirs)
    except Exception as e:
        print(f"An error occurred while listing the databases: {e}")


def print_databases(databases):
    # Print the header
    print("+--------------------+")
    print("| Database           |")
    print("+--------------------+")

    # Print each database name
    for db in databases:
        print(f"| {db.ljust(18)} |")

    # Print the footer
    print("+--------------------+")
    print(f"{len(databases)} rows in set\n")
