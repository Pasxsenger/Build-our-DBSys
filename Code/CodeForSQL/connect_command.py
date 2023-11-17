import os
import csv


def execute_connect_command(handler, command):
    # Check the command structure and extract the necessary parts
    # The format is "Connect [table1] with [table2] based on [common feature];"
    tokens = command.lower().split(" based on ")
    if len(tokens) != 2 or "connect " not in tokens[0] or " with " not in tokens[0]:
        print("Invalid connect command format.")
        return

    # Extract table names and common feature
    table_part = tokens[0].replace("connect ", "").strip()
    table1_name, table2_name = table_part.split("with")
    common_feature_part = tokens[1].strip(";").strip()
    table1_column, table2_column = common_feature_part.split("=")

    # Remove potential extra spaces around table names and column names
    table1_name = table1_name.strip()
    table2_name = table2_name.strip()
    table1_column = table1_column.strip()
    table2_column = table2_column.strip()

    # Check if the path is valid for the operation
    if os.path.basename(os.path.normpath(handler.db_path)) == 'SQL_DB':
        print("Connect command can only be run within a specific database directory.")
        return

    connect_tables(handler, table1_name, table2_name, table1_column, table2_column)


def connect_tables(handler, table1, table2, table1_column, table2_column):
    table1_path = os.path.join(handler.db_path, f"{table1}.csv")
    table2_path = os.path.join(handler.db_path, f"{table2}.csv")
    running_directory = os.path.join(handler.db_path, f"{os.path.basename(handler.db_path)}_running")
    output_file = f"{table1}_connect_{table2}_running.csv"
    output_path = os.path.join(running_directory, output_file)

    # Check if the tables exist
    if not os.path.exists(table1_path) or not os.path.exists(table2_path):
        print("One or both tables do not exist.")
        return

    # Perform block-based nested loop join
    block_size = 50000
    try:
        with open(output_path, 'w', newline='') as output_csv:
            # Read the first table in blocks
            with open(table1_path, 'r', newline='') as table1_csv:
                table1_reader = csv.DictReader(table1_csv)
                table1_headers = table1_reader.fieldnames

                # Verify the joining column exists in table1
                if table1_column not in table1_headers:
                    print(f"Column {table1_column} does not exist in table {table1}.")
                    return

                # Load blocks of table1
                while True:
                    table1_block = [next(table1_reader, None) for _ in range(block_size)]
                    table1_block = [row for row in table1_block if row]  # Filter out None values
                    if not table1_block:
                        break  # Exit if no more data in table1

                    # For each block of table1, read table2 entirely and perform join
                    with open(table2_path, 'r', newline='') as table2_csv:
                        table2_reader = csv.DictReader(table2_csv)
                        table2_headers = table2_reader.fieldnames

                        combined_headers = table1_headers + [h for h in table2_headers if h != table2_column]
                        output_writer = csv.DictWriter(output_csv, fieldnames=combined_headers)
                        output_writer.writeheader()

                        # Verify the joining column exists in table2
                        if table2_column not in table2_headers:
                            print(f"Column {table2_column} does not exist in table {table2}.")
                            return

                        for table2_row in table2_reader:
                            for table1_row in table1_block:
                                if table1_row[table1_column] == table2_row[table2_column]:
                                    # Combine rows without duplicating the common column
                                    joined_row = {**table1_row, **{k: v for k, v in table2_row.items() if k != table2_column}}
                                    output_writer.writerow(joined_row)

        print(f"Connected tables saved to {output_path}\n")

    except Exception as e:
        print(f"An error occurred while connecting tables: {e}")
