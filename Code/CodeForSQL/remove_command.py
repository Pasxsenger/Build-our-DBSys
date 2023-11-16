import os
import csv


def execute_remove_command(handler, command):
    # Parse the command to extract the condition and table name
    # The format is "Remove row with [condition] from [table];"
    tokens = command.lower().split(" from ")
    if len(tokens) != 2 or "remove row with " not in tokens[0]:
        print("Invalid remove command format.")
        return

    condition_part = tokens[0].replace("remove row with ", "").strip()
    table_name = tokens[1].strip().strip(";").strip()

    # Extract the condition
    condition_key, condition_value = condition_part.split('=')
    condition_key = condition_key.strip()
    condition_value = condition_value.strip()

    remove_row(handler, table_name, condition_key, condition_value)


def remove_row(handler, table_name, condition_key, condition_value):
    # Ensure the command is being run in a specific database directory
    if os.path.basename(os.path.normpath(handler.db_path)) == 'SQL_DB':
        print("Remove command can only be run within a specific database directory.")
        return

    table_path = os.path.join(handler.db_path, f"{table_name}.csv")
    running_directory = os.path.join(handler.db_path, f"{os.path.basename(handler.db_path)}_running")
    running_file_path = os.path.join(running_directory, f"{table_name}_running.csv")

    # Check if the table (CSV file) exists
    if not os.path.exists(table_path):
        print(f"Table '{table_name}' does not exist.")
        return

    try:
        # Read and process the file in chunks
        with open(table_path, 'r', newline='') as file, open(running_file_path, 'w', newline='') as running_file:
            reader = csv.DictReader(file)
            headers = reader.fieldnames
            writer = csv.DictWriter(running_file, fieldnames=headers)
            writer.writeheader()

            if condition_key not in headers:
                print(f"Column '{condition_key}' does not exist in the table.")
                return

            chunk_size = 50000  # Define the size of each chunk
            rows_removed = 0
            while True:
                chunk = [row for row in [next(reader, None) for _ in range(chunk_size)] if row]
                if not chunk:
                    break
                for row in chunk:
                    if row[condition_key] == condition_value:
                        rows_removed += 1
                        continue
                    writer.writerow(row)

        if rows_removed == 0:
            print(f"No rows found with {condition_key}={condition_value}.")
            # Clean up the running file as no rows were removed
            os.remove(running_file_path)
            return

        # Replace the original file with the updated running file
        os.replace(running_file_path, table_path)
        print(f"Removed {rows_removed} row(s) with {condition_key}={condition_value} from '{table_name}'.")

    except Exception as e:
        print(f"An error occurred while removing from the table: {e}")