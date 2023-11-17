import os
import csv
from itertools import islice


def execute_sort_command(handler, command):
    # The format is "Sort [table] by [column] in [asc/desc] order;"
    command = ' '.join(command.lower().split())
    parts = command.split(' ')

    if parts[0] != "sort" or "by" not in parts or "in" not in parts or "order" not in parts:
        print("Invalid sort command format.")
        return

    table_name = parts[1]
    sort_column = parts[parts.index("by") + 1]
    sort_order = parts[parts.index("in") + 1]

    if sort_order not in ["asc", "desc"]:
        print("Sort order must be either 'asc' or 'desc'.")
        return

    sort_table(handler, table_name, sort_column, sort_order)


def sort_table(handler, table_name, sort_column, sort_order):
    if os.path.basename(os.path.normpath(handler.db_path)) == 'SQL_DB':
        print("Sort command can only be run within a specific database directory.")
        return

    running_directory = os.path.join(handler.db_path, f"{os.path.basename(handler.db_path)}_running")
    output_path = os.path.join(running_directory, f"sort_{table_name}_{sort_column}_{sort_order}_running.csv")
    if table_name.endswith("_running"):
        table_path = os.path.join(running_directory, f"{table_name}.csv")
    else:
        table_path = os.path.join(handler.db_path, f"{table_name}.csv")

    chunk_size = 50000
    try:
        # Break the file into sorted chunks
        chunk_files = create_sorted_chunks(table_path, sort_column, sort_order, running_directory, chunk_size)

        # Iteratively merge chunks until we have a single sorted file
        pass_num = 0
        while len(chunk_files) > 1:
            chunk_files = multiway_merge(chunk_files, pass_num, sort_column, sort_order, running_directory)
            pass_num += 1

        # Rename the final sorted file to the output file name
        os.rename(chunk_files[0], output_path)

        print(f"Sorted table written to {output_path}")

    except Exception as e:
        print(f"An error occurred while sorting the table: {e}")


def create_sorted_chunks(table_path, sort_column, sort_order, running_directory, chunk_size):
    chunk_files = []
    chunk_number = 0

    with open(table_path, 'r', newline='') as file:
        reader = csv.DictReader(file)
        headers = reader.fieldnames

        if sort_column not in headers:
            raise ValueError(f"Sort column '{sort_column}' does not exist in the table.")

        chunk = list(islice(reader, chunk_size))
        while chunk:
            chunk_sorted = sorted(chunk, key=lambda x: x[sort_column], reverse=(sort_order == 'desc'))
            chunk_file_path = os.path.join(running_directory, f"chunk_{chunk_number}.csv")
            with open(chunk_file_path, 'w', newline='') as chunk_file:
                writer = csv.DictWriter(chunk_file, fieldnames=headers)
                writer.writeheader()
                writer.writerows(chunk_sorted)
            chunk_files.append(chunk_file_path)
            chunk_number += 1
            chunk = list(islice(reader, chunk_size))

    return chunk_files


def multiway_merge(chunk_files, pass_num, sort_column, sort_order, running_directory):
    merged_chunk_files = []
    while chunk_files:
        if len(chunk_files) == 1:  # Only one chunk left, no need to merge, just rename it
            final_chunk = chunk_files.pop()
            final_chunk_new_path = os.path.join(running_directory, f"merged_pass_{pass_num}.csv")
            os.rename(final_chunk, final_chunk_new_path)
            merged_chunk_files.append(final_chunk_new_path)
            break

        chunk1_path, chunk2_path = chunk_files.pop(0), chunk_files.pop(0)
        merged_chunk_path = os.path.join(running_directory, f"merged_pass_{pass_num}_{len(merged_chunk_files)}.csv")
        with open(chunk1_path, 'r', newline='') as file1, open(chunk2_path, 'r', newline='') as file2, \
             open(merged_chunk_path, 'w', newline='') as merged_file:

            reader1, reader2 = csv.DictReader(file1), csv.DictReader(file2)
            writer = csv.DictWriter(merged_file, fieldnames=reader1.fieldnames)
            writer.writeheader()

            # Implement merging of two sorted files into a new sorted file
            merge_sorted_files(reader1, reader2, writer, sort_column, sort_order)

        os.remove(chunk1_path)
        os.remove(chunk2_path)
        merged_chunk_files.append(merged_chunk_path)
    return merged_chunk_files


def merge_sorted_files(reader1, reader2, writer, sort_column, sort_order):
    reverse = sort_order == 'desc'
    row1, row2 = next(reader1, None), next(reader2, None)
    while row1 or row2:
        if row1 and (not row2 or (row1[sort_column] >= row2[sort_column]) ^ reverse):
            writer.writerow(row1)
            row1 = next(reader1, None)
        elif row2:
            writer.writerow(row2)
            row2 = next(reader2, None)
