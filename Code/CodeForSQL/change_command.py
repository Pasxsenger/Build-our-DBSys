import os
import csv


def execute_change_command(handler, command):
    parts = command.lower().split(" for ")
    if len(parts) != 2 or " with " not in parts[1]:
        print("Invalid change command format.")
        return

    change_part, condition_part = parts[0], parts[1]
    if "change" not in change_part or "to" not in change_part or "with" not in condition_part:
        print("Invalid change command format.")
        return

    column_part, value_part = change_part.split(" to ")
    table_part, condition = condition_part.split(" with ")
    column = column_part.replace("change", "").strip()
    new_value = value_part.strip()
    table_name = table_part.strip().strip(";").strip()
    condition_key, condition_value = condition.split('=')
    condition_key = condition_key.strip()
    condition_value = condition_value.strip()

    change_value(handler, table_name, column, new_value, condition_key, condition_value)


def change_value(handler, table_name, column, new_value, condition_key, condition_value):
    if os.path.basename(os.path.normpath(handler.db_path)) == 'SQL_DB':
        print("Change command can only be run within a specific database directory.")
        return

    table_path = os.path.join(handler.db_path, f"{table_name}.csv")
    running_directory = os.path.join(handler.db_path, f"{os.path.basename(handler.db_path)}_running")
    running_file_path = os.path.join(running_directory, f"{table_name}_running.csv")

    if not os.path.exists(table_path):
        print(f"Table '{table_name}' does not exist.")
        return

    try:
        chunk_size = 50000  # Define the size of each chunk
        with open(table_path, 'r', newline='') as file:
            reader = csv.DictReader(file)
            headers = reader.fieldnames

            # Check if the column and condition_key exist in the headers
            if column not in headers:
                print(f"Column '{column}' does not exist in the table.")
                return
            if condition_key not in headers:
                print(f"Condition column '{condition_key}' does not exist in the table.")
                return

        condition_found = False
        # Process the file in chunks
        with open(table_path, 'r', newline='') as file:
            reader = csv.DictReader(file, fieldnames=headers)
            chunk = []  # Initialize an empty list for the chunk
            for row in reader:
                if row[condition_key] == condition_value:
                    condition_found = True
                    row[column] = new_value
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    write_chunk(chunk, headers, running_file_path, mode='a')
                    chunk = []  # Reset the chunk for the next batch

            # Write any remaining rows in the final chunk
            if chunk:
                write_chunk(chunk, headers, running_file_path, mode='a')

        if not condition_found:
            print(f"No row found with {condition_key} = {condition_value}.")
            return

        # Replace the original file with the updated running file
        os.replace(running_file_path, table_path)
        print(f"Value changed in table '{table_name}' for rows where {condition_key}={condition_value}.")

    except Exception as e:
        print(f"An error occurred while changing the value: {e}")


def write_chunk(chunk, headers, running_file_path, mode='w'):
    # Write chunk to file using the given mode
    with open(running_file_path, mode, newline='') as temp_file:
        writer = csv.DictWriter(temp_file, fieldnames=headers)
        writer.writerows(chunk)
