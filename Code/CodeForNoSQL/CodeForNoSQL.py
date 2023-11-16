import json
import os
import re
import shutil
import time

class Database:
    def __init__(self, db_path):
        self.db_path = db_path

    def query(self, query_string):
        # Use QueryParser to parse the query_string
        parsed_query = QueryParser().parse(query_string)

        # Determine the type of operation and call the respective method
        operation = parsed_query['operation']
        if operation == 'create_database':
            return self.create_database(parsed_query['database_name'])
        elif operation == 'switch_database':
            return self.switch_database(parsed_query['database_name'])
        elif operation == 'create_collection':
            return self.create_collection(parsed_query['collection_name'], parsed_query['fields'])
        elif operation == 'drop_collection':
            return self.drop_collection(parsed_query['collection_name'])
        elif operation == 'drop_database':
            return self.drop_database(parsed_query['database_name'])
        elif operation == 'show_collections':
            return self.show_collections()
        elif operation == 'show_databases':
            return self.show_databases()
        elif operation == 'retrieve_data':
            return self.retrieve_data(parsed_query['collection'], parsed_query['fields'], parsed_query['condition'])
        elif operation == 'join_collections':
            return self.join(parsed_query['collection1'], parsed_query['collection2'], parsed_query['common_feature'])
        elif operation == 'grouping_aggregation':
            return self.grouping(parsed_query['collection'], parsed_query['feature'], parsed_query['group_by'], parsed_query['aggregation_method'])
        elif operation == 'sort_data':
            return self.ordering(parsed_query['collection'], parsed_query['sort_field'], parsed_query['order'])
        elif operation == 'insert_data':
            return self.insert_data(parsed_query['collection'], parsed_query['data'])
        elif operation == 'delete_data':
            return self.delete_data(parsed_query['collection'], parsed_query['condition'])
        elif operation == 'update_data':
            return self.update_data(parsed_query['collection'], parsed_query['field'], parsed_query['new_value'], parsed_query['condition'])
        else:
            raise ValueError("Unknown operation requested.")
        # Additional query types can be added here
        
    def create_database(self, database_name):
        # Create a new database (folder)
        db_path = os.path.join(self.db_path, database_name)
        os.makedirs(db_path, exist_ok=True)
        return f"Database '{database_name}' created."

    def switch_database(self, database_name):
        # Switch to an existing database
        self.current_database = database_name
        return f"Switched to database '{database_name}'."
       
    def drop_collection(self, collection_name):
        """
        Deletes a specified collection within the current database.
        :param collection_name: The name of the collection to be deleted.
        """
        if self.current_database is None:
            raise ValueError("No database selected.")

        collection_path = os.path.join(self.db_path, "NoSQL_DB", self.current_database, f"{collection_name}.json")

        if os.path.exists(collection_path):
            os.remove(collection_path)
            return f"Collection '{collection_name}' has been deleted."
        else:
            raise ValueError(f"Collection '{collection_name}' does not exist.")
        
    def drop_database(self, database_name):
        """
        Deletes an entire database (a directory and its contents).
        :param database_name: The name of the database to be deleted.
        """
        db_path = os.path.join(self.db_path, "NoSQL_DB", database_name)

        if os.path.exists(db_path):
            shutil.rmtree(db_path)
            return f"Database '{database_name}' has been deleted."
        else:
            raise ValueError(f"Database '{database_name}' does not exist.")

    def create_collection(self, collection_name, fields):
        # Create a new collection (JSON file) within the current database
        if self.current_database is None:
            raise ValueError("No database selected.")
        collection_path = os.path.join(self.db_path, self.current_database, f"{collection_name}.json")
        with open(collection_path, 'w') as file:
            json.dump([], file)  # Create an empty JSON array
        return f"Collection '{collection_name}' created with fields {fields}."
    
    def _matches_condition(self, json_object, fields, condition):
        # Example condition format: "Age > 30"
        pattern = r'(\w+) ([><=]) (.+)'
        match = re.match(pattern, condition)
        if not match:
            raise ValueError("Invalid condition format.")

        field, operator, value = match.groups()

        # Ensure the field is part of the projection
        if field not in fields:
            raise ValueError(f"Field '{field}' not found in the projection fields.")

        # Convert value to appropriate type
        if field in json_object:
            json_value = json_object[field]
            if isinstance(json_value, int) or isinstance(json_value, float):
                value = float(value)
            elif isinstance(json_value, str):
                value = str(value)

            # Evaluate condition
            if operator == '=' and json_value == value:
                return True
            elif operator == '>' and json_value > value:
                return True
            elif operator == '<' and json_value < value:
                return True

        return False
    
    def _display_results(self, file_path):
        # Read and display the results from the output file
        results = []
        for json_object in stream_read_json(file_path):
            results.append(json_object)
        return results
    
    def retrieve_data(self, collection_name, fields, condition):
        # Generating a descriptive file name for the query results
        fields_str = '_'.join(fields).replace(' ', '_')
        condition_str = condition.replace(' ', '_').replace('>', 'gt').replace('<', 'lt').replace('=', 'eq')
        output_filename = f"{collection_name}_retrieve_data_{fields_str}_{condition_str}_running.json"
        output_path = os.path.join(self.db_path, "NoSQL_DB", self.current_database, "NoSQL_running", output_filename)

        # Processing the query
        with open(output_path, 'w') as output_file:
            output_file.write('[')
            first_item_written = False
            for json_object in stream_read_json(os.path.join(self.db_path, "NoSQL_DB", self.current_database, f"{collection_name}.json")):
                if self._matches_condition(json_object, fields, condition):
                    if first_item_written:
                        output_file.write(',')
                    else:
                        first_item_written = True
                    projected_json_object = {field: json_object[field] for field in fields}
                    output_file.write(json.dumps(projected_json_object))
            output_file.write(']')

        # Displaying the results
        return self._display_results(output_path)
    

    def insert_data(self, collection_name, new_data):
        """
        Inserts new data into a specified collection.
        :param collection_name: The name of the collection to insert data into.
        :param new_data: A dictionary representing the new data to be inserted.
        """
        collection_path = os.path.join(self.db_path, "NoSQL_DB", self.current_database, f"{collection_name}.json")

        # Check if the collection file exists
        if not os.path.exists(collection_path):
            raise ValueError(f"Collection '{collection_name}' does not exist.")

        # Append the new data to the collection
        with open(collection_path, 'a') as file:
            # If the file is not empty, we need to remove the last ']' and add a comma
            file.seek(0, os.SEEK_END)
            if file.tell() > 2:  # Check if the file is not just an empty JSON array '[]'
                file.seek(-1, os.SEEK_END)
                file.truncate()  # Remove the last ']'
                file.write(',\n')
            else:
                file.seek(0, os.SEEK_SET)

            file.write(json.dumps(new_data) + '\n]')

        return "Data inserted successfully."
    
    def update_data(self, collection_name, field, new_value, condition):
        """
        Updates data in a specified collection based on a condition.
        :param collection_name: The name of the collection to update data in.
        :param field: The field (column) in the collection to be updated.
        :param new_value: The new value to be set for the field.
        :param condition: A string representing the condition to select records for update.
        """
        if self.current_database is None:
            raise ValueError("No database selected.")

        collection_path = os.path.join(self.db_path, "NoSQL_DB", self.current_database, f"{collection_name}.json")
        condition_str = condition.replace(' ', '_').replace('>', 'gt').replace('<', 'lt').replace('=', 'eq')
        temp_filename = f"{collection_name}_update_data_{field}_to_{new_value}_{condition_str}_running.json"
        temp_path = os.path.join(self.db_path, "NoSQL_DB", self.current_database, "DatabaseB_running", temp_filename)

        if not os.path.exists(collection_path):
            raise ValueError(f"Collection '{collection_name}' does not exist.")

        with open(temp_path, 'w') as write_file:
            write_file.write('[')
            first_item_written = False
            for batch in stream_read_json(collection_path, batch_size=1000):  # Assuming batch size of 1000
                for json_object in batch:
                    if self._matches_condition(json_object, [field], condition):
                        json_object[field] = new_value

                    if first_item_written:
                        write_file.write(',')
                    else:
                        first_item_written = True
                    write_file.write(json.dumps(json_object))
            write_file.write(']')

        # Replace the original collection file with the updated one
        shutil.move(temp_path, collection_path)

        return "Data updated successfully."

    def delete_data(self, collection_name, condition):
        """
        Deletes data from a specified collection based on a condition.
        :param collection_name: The name of the collection to delete data from.
        :param condition: A string representing the condition for deleting data.
        """
        collection_path = os.path.join(self.db_path, "NoSQL_DB", self.current_database, f"{collection_name}.json")
        condition_filename = condition.replace(' ', '_').replace('>', 'gt').replace('<', 'lt').replace('=', 'eq')
        temp_path = os.path.join(self.db_path, "NoSQL_DB", self.current_database, "NoSQL_running", f"{collection_name}_delete_data_{condition_filename}_running.json")

        if not os.path.exists(collection_path):
            raise ValueError(f"Collection '{collection_name}' does not exist.")

        # Process the deletion and write to the temporary file
        with open(temp_path, 'w') as temp_file:
            temp_file.write('[')
            first_item_written = False
            for json_object in stream_read_json(collection_path):
                if not self._matches_condition(json_object, json_object.keys(), condition):
                    if first_item_written:
                        temp_file.write(',')
                    else:
                        first_item_written = True
                    temp_file.write(json.dumps(json_object))
            temp_file.write(']')

        # Replace the original collection file with the updated one
        shutil.move(temp_path, collection_path)

        return "Data deleted successfully."
    

    def join(self, query_details):
        # Logic for join operation
        pass

    def grouping_aggregation(self, collection_name, group_by_field, aggregation_field, aggregation_method):
        """
        Groups data and performs aggregation on a specified field, including aggregating over all records.
        :param collection_name: The name of the collection to perform aggregation.
        :param group_by_field: The field to group the data by, or 'all' for no grouping.
        :param aggregation_field: The field to apply the aggregation method.
        :param aggregation_method: The aggregation method (e.g., sum, average, min, max, count).
        """
        if self.current_database is None:
            raise ValueError("No database selected.")

        collection_path = os.path.join(self.db_path, "NoSQL_DB", self.current_database, f"{collection_name}.json")
        grouped_data = {}
        aggregate_over_all = (group_by_field == 'all')

        # Processing the collection in batches
        for batch in stream_read_json(collection_path, batch_size=1000):  # Assuming batch size of 1000
            for json_object in batch:
                group_key = 'All' if aggregate_over_all else json_object.get(group_by_field)
                if group_key is not None:
                    if group_key not in grouped_data:
                        grouped_data[group_key] = {'total': 0, 'count': 0, 'min': float('inf'), 'max': float('-inf')}
                    if aggregation_field in json_object:
                        value = json_object[aggregation_field]
                        if aggregation_method in ['sum', 'average']:
                            grouped_data[group_key]['total'] += value
                        if aggregation_method in ['average', 'min', 'max', 'count']:
                            grouped_data[group_key]['count'] += 1
                        if aggregation_method == 'min':
                            grouped_data[group_key]['min'] = min(grouped_data[group_key]['min'], value)
                        if aggregation_method == 'max':
                            grouped_data[group_key]['max'] = max(grouped_data[group_key]['max'], value)

        # Preparing detailed results
        detailed_results = []
        for key, values in grouped_data.items():
            result = {group_by_field if not aggregate_over_all else 'Aggregation': key}
            if aggregation_method == 'average':
                result[f"{aggregation_field}_Average"] = values['total'] / values['count'] if values['count'] > 0 else None
            elif aggregation_method == 'sum':
                result[f"{aggregation_field}_Sum"] = values['total']
            elif aggregation_method == 'min':
                result[f"{aggregation_field}_Min"] = values['min'] if values['min'] != float('inf') else None
            elif aggregation_method == 'max':
                result[f"{aggregation_field}_Max"] = values['max'] if values['max'] != float('-inf') else None
            detailed_results.append(result)

        return detailed_results

    def sort_data(self, query_details):
        # Logic for ordering operation
        pass

    # Additional methods for insert, delete, update, etc. go here


class QueryParser:
    def parse(self, query_string):
        # Determine the type of query and call the respective method
        if query_string.startswith('Create a new database named'):
            return self.parse_create_database(query_string)
        elif query_string.startswith('Switch to database'):
            return self.parse_switch_database(query_string)
        elif query_string.startswith('Set up a new collection'):
            return self.parse_create_collection(query_string)
        elif query_string.startswith('Show'):
            if query_string.endswith('where'):
                return self.parse_retrieve_data(query_string)
            elif query_string == 'Tables' or query_string == 'Collections':
                return {"operation": "show_collections"}
            elif query_string == 'Databases':
                return {"operation": "show_databases"}
        elif query_string.startswith('Connect'):
            return self.parse_join_collections(query_string)
        elif query_string.startswith('Summarize'):
            return self.parse_grouping_aggregation(query_string)
        elif query_string.startswith('Sort'):
            return self.parse_sort_data(query_string)
        elif query_string.startswith('Add'):
            return self.parse_insert_data(query_string)
        elif query_string.startswith('Remove'):
            return self.parse_delete_data(query_string)
        elif query_string.startswith('Drop'):
            return self.parse_drop_collection(query_string)
        elif query_string.startswith('Change'):
            return self.parse_update_data(query_string)
        elif query_string.startswith('Delete database'):
            return self.parse_drop_database(query_string)
        else:
            raise ValueError("Unknown query format.")

    def parse_create_database(self, query_string):
        match = re.match(r'Create a new database named (\w+)', query_string)
        if match:
            return {"operation": "create_database", "database_name": match.group(1)}
        else:
            raise ValueError("Invalid query format for creating a database.")

    def parse_switch_database(self, query_string):
        match = re.match(r'Switch to database (\w+)', query_string)
        if match:
            return {"operation": "switch_database", "database_name": match.group(1)}
        else:
            raise ValueError("Invalid query format for switching database.")

    def parse_create_collection(self, query_string):
        match = re.match(r'Set up a new collection named (\w+) with (.+)', query_string)
        if match:
            fields = match.group(2).split(', ')
            return {"operation": "create_collection", "collection_name": match.group(1), "fields": fields}
        else:
            raise ValueError("Invalid query format for creating a collection.")
        
    def parse_drop_collection(self, query_string):
        # Example: "Drop collection Employees"
        pattern = r'Drop collection (.+)'
        match = re.match(pattern, query_string)
        if match:
            collection_name = match.group(1)
            return {"operation": "drop_collection", "collection_name": collection_name}
        else:
            raise ValueError("Invalid query format for dropping a collection.")
        
    def parse_drop_database(self, query_string):
        # Example: "Delete database EmployeeRecords"
        pattern = r'Delete database (.+)'
        match = re.match(pattern, query_string)
        if match:
            database_name = match.group(1)
            return {"operation": "drop_database", "database_name": database_name}
        else:
            raise ValueError("Invalid query format for dropping a database.")
        
    def parse_show_collections(self):
        # No additional information needed for showing collections
        return {"operation": "show_collections"}

    def parse_show_databases(self):
        # No additional information needed for showing databases
        return {"operation": "show_databases"}

    def parse_retrieve_data(self, query_string):
        # Example: "Show names, ages of employees where age > 30"
        pattern = r'Show (.+) of (.+) where (.+)'
        match = re.match(pattern, query_string)
        if match:
            fields = [field.strip() for field in match.group(1).split(',')]
            collection = match.group(2)
            condition = match.group(3)
            return {"operation": "retrieve_data", "fields": fields, "collection": collection, "condition": condition}
        else:
            raise ValueError("Invalid query format for retrieving data.")
        
    def parse_join_collections(self, query_string):
        # Example: "Connect employees with departments using employees.department = departments.ID"
        pattern = r'Connect (.+) with (.+) using (.+)'
        match = re.match(pattern, query_string)
        if match:
            collection1, collection2, common_feature = match.groups()
            return {"operation": "join_collections", "collection1": collection1, "collection2": collection2, "common_feature": common_feature}
        else:
            raise ValueError("Invalid query format for joining collections.")

    def parse_grouping_aggregation(self, query_string):
        # Example: "Summarize salary on name from employees using average"
        pattern = r'Summarize (.+) on (.+) from (.+) using (.+)'
        match = re.match(pattern, query_string)
        if match:
            feature, group_by, collection, aggregation_method = match.groups()
            return {"operation": "grouping_aggregation", "feature": feature, "group_by": group_by, "collection": collection, "aggregation_method": aggregation_method}
        else:
            raise ValueError("Invalid query format for grouping and aggregation.")

    def parse_sort_data(self, query_string):
        # Example: "Sort employees by salary in desc order"
        pattern = r'Sort (.+) by (.+) in (asc|desc) order'
        match = re.match(pattern, query_string)
        if match:
            collection, sort_field, order = match.groups()
            return {"operation": "sort_data", "collection": collection, "sort_field": sort_field, "order": order}
        else:
            raise ValueError("Invalid query format for sorting data.")
        
    def parse_insert_data(self, query_string):
        # Example: "Add John Doe, Marketing, 50000 to employees"
        pattern = r'Add (.+) to (.+)'
        match = re.match(pattern, query_string)
        if match:
            data, collection = match.groups()
            # Splitting and trimming each data field
            data_fields = [field.strip() for field in data.split(',')]
            return {"operation": "insert_data", "data": data_fields, "collection": collection}
        else:
            raise ValueError("Invalid query format for inserting data.")

        
    def parse_delete_data(self, query_string):
        # Example: "Remove row with ID='123' from employees"
        pattern = r'Remove row with (.+) from (.+)'
        match = re.match(pattern, query_string)
        if match:
            condition, collection = match.groups()
            return {"operation": "delete_data", "condition": condition, "collection": collection}
        else:
            raise ValueError("Invalid query format for deleting data.")


    def parse_update_data(self, query_string):
        # Example: "Change department to Sales for employees with ID='123'"
        pattern = r'Change (.+) to (.+) for (.+) with (.+)'  # Assumption: 'for' precedes the collection name, and 'with' precedes the condition
        match = re.match(pattern, query_string)
        if match:
            field, new_value, collection, condition = match.groups()
            return {"operation": "update_data", "field": field, "new_value": new_value, "collection": collection, "condition": condition}
        else:
            raise ValueError("Invalid query format for updating data.")










class CLI:
    def __init__(self, database):
        self.database = database

    def start(self):
        # Start the CLI and process commands
        pass


def stream_read_json(file_path, batch_size=1):
    """
    Generator function to read multiple JSON objects at a time from a file.
    Each line in the file should be a separate JSON object.

    :param file_path: Path to the JSON file.
    :param batch_size: Number of JSON objects to read at a time.
    """
    with open(file_path, 'r') as file:
        batch = []
        for line in file:
            batch.append(json.loads(line))
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:  # Yield the last batch if it's not empty
            yield batch


def stream_write_results(file_path, json_objects):
    """
    Function to write a batch of JSON objects to a file.

    :param file_path: Path to the JSON file where data will be written.
    :param json_objects: A list of JSON objects to write.
    """
    with open(file_path, 'a') as file:
        # Writing each JSON object in the list to the file
        for obj in json_objects:
            file.write(json.dumps(obj) + '\n')


# Example usage
if __name__ == "__main__":
    db = Database('/path/to/db')
    cli = CLI(db)
    cli.start()
