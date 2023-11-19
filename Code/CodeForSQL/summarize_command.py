import os
import csv
from collections import defaultdict


def execute_summarize_command(handler, command):
    # Lowercase the command and remove extra spaces
    # The format is "Summarize [column] on [group by column]/[all] from [table] using [aggregation method](numeric only: avg/sum)(count/min/max);"
    command = ' '.join(command.lower().split())
    parts = command.split(' ')

    # Validate the format of the command
    if parts[0] != "summarize" or "on" not in parts or "from" not in parts or "using" not in parts:
        print("Invalid summarize command format.")
        return

    # Extract relevant parts of the command
    column_to_summarize = parts[1]
    group_by = parts[parts.index("on") + 1]
    table_name = parts[parts.index("from") + 1]
    aggregation_method = parts[parts.index("using") + 1].replace(';', '')

    summarize_table(handler, table_name, column_to_summarize, group_by, aggregation_method)


def summarize_table(handler, table_name, column_to_summarize, group_by, aggregation_method):
    if os.path.basename(os.path.normpath(handler.db_path)) == 'SQL_DB':
        print("Summarize command can only be run within a specific database directory.")
        return

    running_directory = os.path.join(handler.db_path, f"{os.path.basename(handler.db_path)}_running")
    summarize_path = os.path.join(running_directory, f"summarize_{table_name}_group_by_{group_by}_{aggregation_method}_running.csv")
    if table_name.endswith("_running"):
        table_path = os.path.join(running_directory, f"{table_name}.csv")
    else:
        table_path = os.path.join(handler.db_path, f"{table_name}.csv")

    # Initialize aggregation data structure
    aggregation_data = defaultdict(list)

    try:
        with open(table_path, 'r', newline='') as file:
            reader = csv.DictReader(file)
            headers = reader.fieldnames

            # Validate the column names
            if column_to_summarize not in headers or (group_by != 'all' and group_by not in headers):
                print("One or more columns do not exist in the table.")
                return

            # Read and process in blocks
            block_size = 50000
            while True:
                block = [next(reader, None) for _ in range(block_size)]
                block = list(filter(None, block))  # Remove None in case we reach the end of file
                if not block:
                    break  # End of file

                for row in block:
                    key = row[group_by] if group_by != 'all' else 'all'
                    value = row[column_to_summarize]

                    if value == "" or value == " ":
                        continue

                    if aggregation_method in ['avg', 'sum']:
                        try:
                            value = float(value)
                        except ValueError:
                            print(f"Non-numeric data encountered in numeric-only operation: {value}")
                            return
                    aggregation_data[key].append(value)

        # Perform final aggregation
        results = perform_aggregation(aggregation_data, aggregation_method)

        # Write the results to the summarize file
        write_results(summarize_path, results, column_to_summarize, group_by, aggregation_method)

    except Exception as e:
        print(f"An error occurred while summarizing the table: {e}")


def perform_aggregation(aggregation_data, method):
    results = {}
    for key, values in aggregation_data.items():
        if method == 'avg':
            numeric_values = [float(value) for value in values]
            results[key] = sum(numeric_values) / len(numeric_values) if numeric_values else 0
        elif method == 'sum':
            numeric_values = [float(value) for value in values]
            results[key] = sum(numeric_values)
        elif method == 'count':
            results[key] = len(values)
        elif method == 'min':
            results[key] = min(values)
        elif method == 'max':
            results[key] = max(values)
    return results


def write_results(file_path, results, column_to_summarize, group_by, aggregation_method):
    with open(file_path, 'w', newline='') as file:
        if group_by == 'all':
            headers = [f"{aggregation_method}({column_to_summarize})"]
        else:
            headers = [group_by, f"{aggregation_method}({column_to_summarize})"]
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for key, value in results.items():
            if group_by == 'all':
                writer.writerow({headers[0]: value})
            else:
                writer.writerow({headers[0]: key, headers[1]: value})
