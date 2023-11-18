import importlib
import os
import shutil


class SQLHandler:
    def __init__(self, db_path='../SQL_DB/'):
        self.db_path = db_path

    def handle_commands(self):
        print("SQL Handler active. Type 'exit' to return to the main menu.")
        command = ""
        while True:
            line = input("SQL_DB > " if not command else "... ")
            command += line
            # if command.lower().strip() == 'exit':
            #     # Check if second-lowest path is 'SQL_DB'
            #     if os.path.basename(os.path.dirname(self.db_path)) == "SQL_DB":
            #         running_directory = os.path.join(self.db_path, f"{os.path.basename(self.db_path)}_running")
            #         # Delete the running_directory if it exists
            #         if os.path.exists(running_directory):
            #             shutil.rmtree(running_directory)
            #             # Create a new running_directory
            #             os.makedirs(running_directory)
            #     return

            if command.lower().strip() == 'exit':
                # Identify SQL_DB directory
                current_path = self.db_path
                while os.path.basename(current_path) != "SQL_DB" and os.path.basename(os.path.dirname(current_path)) != "SQL_DB":
                    current_path = os.path.dirname(current_path)

                if os.path.basename(current_path) == "SQL_DB":
                    sql_db_directory = current_path
                else:
                    sql_db_directory = os.path.dirname(current_path)

                # Iterate through each subdirectory in SQL_DB
                for subdir in os.listdir(sql_db_directory):
                    subdir_path = os.path.join(sql_db_directory, subdir)
                    running_directory = os.path.join(subdir_path, f"{subdir}_running")
                    if os.path.isdir(subdir_path) and os.path.exists(running_directory):
                        # Delete the running_directory if it exists
                        shutil.rmtree(running_directory)
                        # Optionally, recreate the running_directory
                        os.makedirs(running_directory)
                return

            if command.endswith(';'):
                command = command[:-1].strip()  # Remove the semicolon and strip whitespace
                self.process_command(command)
                command = ""  # Reset command for the next input

    def process_command(self, command):
        # Split the command into words and identify the command keyword
        words = command.split()
        if not words:
            print("No command entered.")
            return

        # Mapping of command keywords to module names
        command_modules = {
            'create': 'create_database_command',
            'switch': 'switch_database_command',
            'delete': 'delete_database_command',
            'databases': 'show_database_command',

            'set': 'set_table_command',
            'drop': 'drop_table_command',
            'tables': 'show_table_command',

            'add': 'add_command',
            'remove': 'remove_command',
            'change': 'change_command',

            'show': 'show_command',
            'connect': 'connect_command',
            'summarize': 'summarize_command',
            'sort': 'sort_command'
        }

        keyword = words[0].lower()
        if keyword in command_modules:
            module_name = command_modules[keyword]
            try:
                # The import path should be relative to the current file
                command_module = importlib.import_module(f'.{module_name}', package='CodeForSQL')
                execute_command = getattr(command_module, f'execute_{keyword}_command')
                execute_command(self, command)
            except ModuleNotFoundError as e:
                print(f"Command module '{module_name}' not found: {e}")
            except AttributeError as e:
                print(f"Command function for '{keyword}' not found: {e}")
        else:
            print(f"Command '{command}' is not recognized.")
