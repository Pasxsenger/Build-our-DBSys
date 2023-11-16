import os
import csv
from itertools import islice


def execute_add_command(handler, command):
    # Parse the command to extract the row data and table name
    # The format is "Add [row] to [table];"
    command = command.lower()
    tokens = command.split(" to ")
    if len(tokens) != 2 or not tokens[0].startswith("add "):
        print("Invalid add command format.")
        return

    # Extract the row data and the table name, ensuring all are lowercase
    row_data = tokens[0][4:].strip().strip(";")  # Remove 'Add ' and trailing semicolon
    row_data = [item.strip() for item in row_data.split(",")]  # Split the row data by commas
    table_name = tokens[1].strip().strip(";")  # Remove trailing semicolon from table name

    add_row(handler, table_name, row_data)

def add_row(handler, table_name, row_data):
    # Ensure the command is being run in a specific database directory, not directly under SQL_DB
    if os.path.basename(os.path.normpath(handler.db_path)) == 'SQL_DB':
        print("Add command can only be run within a specific database directory.")
        return

    table_path = os.path.join(handler.db_path, f"{table_name}.csv")
    running_directory = os.path.join(handler.db_path, f"{os.path.basename(handler.db_path)}_running")
    running_file_path = os.path.join(running_directory, f"{table_name}_running.csv")

    if not os.path.exists(table_path):
        print(f"Table '{table_name}' does not exist.")
        return

    try:
        chunk_size = 50000  # Define the size of each chunk
        current_chunk_size = 0  # Current size of the chunk being read

        with open(table_path, 'r', newline='') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Read the headers directly from the file

            # Check if the number of data items matches the number of columns
            if len(row_data) != len(headers):
                print("The number of data items does not match the number of columns in the table.")
                return

            # Write headers to the running file
            with open(running_file_path, 'w', newline='') as running_file:
                writer = csv.writer(running_file)
                writer.writerow(headers)  # Write the headers to the running file

            # Write existing data by chunks
            with open(running_file_path, 'a', newline='') as running_file:
                writer = csv.writer(running_file)

                for row in reader:
                    writer.writerow(row)
                    current_chunk_size += 1
                    if current_chunk_size >= chunk_size:
                        running_file.flush()  # Flush the contents to disk
                        current_chunk_size = 0  # Reset the chunk size

            # Append the new row
            with open(running_file_path, 'a', newline='') as running_file:
                writer = csv.writer(running_file)
                writer.writerow(row_data)

        # Replace the original file with the updated running file
        os.replace(running_file_path, table_path)
        print(f"Row added to table '{table_name}'.")

    except Exception as e:
        print(f"An error occurred while adding to the table: {e}")

