import os
import csv


def execute_show_command(handler, command):
    # Initial command parsing and format checks
    command = command.lower()
    command = command.strip().strip(";")
    if not command.startswith("show ") or " of " not in command:
        print("Invalid show command format.")
        return

    # Extract columns, table name, conditions, and line range
    show_part, rest = command.split(" of ")
    columns_part = show_part.replace("show ", "").strip()

    table_part = rest
    conditions = []
    line_range = None
    if " where " in rest:
        table_part, condition_part = rest.split(" where ")
        if " line " in condition_part:
            condition_part, line_part = condition_part.split(" line ")
            line_range = parse_line_range(line_part)
        conditions = parse_conditions(condition_part)
    elif " line " in rest:
        table_part, line_part = rest.split(" line ")
        line_range = parse_line_range(line_part)

    table_name = table_part.strip().split()[0].strip()
    columns = parse_columns(columns_part)

    # Check the line range to make sure start <= end
    if line_range:
        if line_range[0] > line_range[1]:
            print("Invalid line range.")
            return

    show_table(handler, table_name, columns, conditions, line_range)


def parse_columns(columns_part):
    if columns_part.lower() == 'all':
        return 'all'
    return [col.strip() for col in columns_part.split(',')]


def parse_conditions(condition_part):
    conditions = condition_part.split(" and ")
    return [condition.strip() for condition in conditions]


def parse_line_range(line_part):
    start, end = line_part.split("-")
    return [int(start.strip()), int(end.strip())]


def show_table(handler, table_name, columns, conditions, line_range):
    # Check if the command can be run in the current directory
    if os.path.basename(os.path.normpath(handler.db_path)) == 'SQL_DB':
        print("Show command can only be run within a specific database directory.")
        return

    if table_name.endswith("_running"):
        # running_directory = handler.db_path
        running_directory = os.path.join(handler.db_path, f"{os.path.basename(handler.db_path)}_running")
        table_path = os.path.join(running_directory, f"{table_name}.csv")
        running_file_path = os.path.join(running_directory, f"show_{table_name}.csv")
    else:
        table_path = os.path.join(handler.db_path, f"{table_name}.csv")
        running_directory = os.path.join(handler.db_path, f"{os.path.basename(handler.db_path)}_running")
        running_file_path = os.path.join(running_directory, f"{table_name}_running.csv")

    if not os.path.exists(table_path):
        print(f"Table '{table_name}' does not exist.")
        return

    try:
        # Process the table in chunks
        with open(table_path, 'r', newline='') as file, open(running_file_path, 'w', newline='') as running_file:
            reader = csv.DictReader(file)
            headers = reader.fieldnames

            # Validate columns and conditions
            if columns != 'all' and any(col not in headers for col in columns):
                print("One or more columns do not exist in the table.")
                return
            for condition in conditions:
                if not any(condition.startswith(header) for header in headers):
                    print(f"One or more condition keys do not exist in the table: {condition}")
                    return

            # Initialize the CSV DictWriter with the selected columns
            writer = csv.DictWriter(running_file, fieldnames=headers if columns == 'all' else columns)
            writer.writeheader()

            # Read and write in chunks, checking conditions
            chunk_counter = 0
            for row in reader:
                if all(check_condition(row, condition) for condition in conditions):
                    if columns == 'all':
                        writer.writerow(row)
                    else:
                        writer.writerow({col: row[col] for col in columns})
                chunk_counter += 1
                if chunk_counter >= 50000:
                    running_file.flush()
                    chunk_counter = 0

        # Print the output from the running file
        print_output(running_file_path, line_range)

        # Clean up: Delete the running file
        os.remove(running_file_path)

    except Exception as e:
        print(f"An error occurred while showing the table: {e}")


def check_condition(row, condition):
    key, operator, value = parse_condition(condition)
    return compare(row.get(key, ''), operator, value)


def parse_condition(condition):
    operators = ['>=', '<=', '!=', '=', '>', '<']
    for op in operators:
        if op in condition:
            parts = condition.split(op)
            return parts[0].strip(), op, parts[1].strip()
    raise ValueError(f"Invalid operator in condition: {condition}")


def compare(item, operator, value):
    if operator == '=':
        return item == value
    elif operator == '!=':
        return item != value
    elif operator == '>':
        return item > value
    elif operator == '<':
        return item < value
    elif operator == '>=':
        return item >= value
    elif operator == '<=':
        return item <= value
    else:
        raise ValueError(f"Invalid operator: {operator}")


def print_output(file_path, line_range=None):
    # Read the data from the file
    with open(file_path, 'r', newline='') as file:
        reader = csv.reader(file)
        all_rows = list(reader)

    # Calculate column widths by finding the max length of items in each column including the header
    column_widths = [max(len(str(item)) for item in col) for col in zip(*all_rows)]
    num_columns = len(column_widths)

    # Define a function to print a divider
    def print_divider(column_widths):
        line = '+' + '+'.join('-' * (width + 2) for width in column_widths) + '+'
        print(line)

    # Define a function to print a row of data
    def print_row(row, column_widths):
        row_line = '|' + '|'.join(f' {item.ljust(width)} ' for item, width in zip(row, column_widths)) + '|'
        print(row_line)

    # Print the header
    print_divider(column_widths)
    print_row(all_rows[0], column_widths)
    print_divider(column_widths)

    # Print the data rows
    row_count = 0
    for i, row in enumerate(all_rows[1:], start=1):
        if line_range and (i < line_range[0] or i > line_range[1]):
            continue
        print_row(row, column_widths)
        row_count += 1
        if line_range and i == line_range[1]:
            break

    # Print the footer divider
    print_divider(column_widths)
    print(f"{row_count} rows in set")
