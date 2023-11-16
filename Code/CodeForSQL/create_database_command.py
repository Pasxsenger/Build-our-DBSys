import os


def execute_create_command(handler, command):
    # Parse the command to extract the database name
    tokens = command.split()
    # Assuming the format is "create a new database named [database_name];"
    if len(tokens) == 6 and tokens[1].lower() == "a" and tokens[2].lower() == "new" and tokens[3].lower() == "database" and tokens[4].lower() == "named":
        db_name = tokens[5]
        create_database(db_name)
    else:
        print("Invalid create command.")


def create_database(db_name):
    # Define the path for the new database (folder)
    db_path = os.path.join('../SQL_DB', db_name)
    # full_path = os.path.join(db_path, db_name)
    try:
        # Create the directory if it doesn't exist
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        # if not os.path.exists(full_path):
        #     os.makedirs(full_path)
            print(f"Database '{db_name}' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")
    except Exception as e:
        print(f"An error occurred while creating the database: {e}")
