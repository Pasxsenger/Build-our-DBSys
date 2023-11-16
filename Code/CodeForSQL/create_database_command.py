import os


def execute_create_command(handler, command):
    # Parse the command to extract the database name
    tokens = command.split()
    # The format is "create a new database named [database_name];"
    if len(tokens) == 6 and tokens[1].lower() == "a" and tokens[2].lower() == "new" and tokens[3].lower() == "database" and tokens[4].lower() == "named":
        db_name = tokens[5].lower()
        create_database(db_name)
    else:
        print("Invalid create command.")


def create_database(db_name):
    # Define the path for the new database (folder)
    db_path = os.path.join('../SQL_DB', db_name)
    running_path = os.path.join(db_path, f"{db_name}_running")  # Path for the running folder

    try:
        # Create the directory if it doesn't exist
        if not os.path.exists(db_path):
            os.makedirs(db_path)
            print(f"Database '{db_name}' created successfully.")

            # Create the [database_name]_running subdirectory
            if not os.path.exists(running_path):
                os.makedirs(running_path)
                # print(f"Subdirectory '{db_name}_running' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")
    except Exception as e:
        print(f"An error occurred while creating the database: {e}")
