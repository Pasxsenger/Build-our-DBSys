import importlib


class SQLHandler:
    def __init__(self, db_path='../SQL_DB/'):
        self.db_path = db_path

    def handle_commands(self):
        print("SQL Handler active. Type 'exit' to return to the main menu.")
        command = ""
        while True:
            line = input("SQL_DB > " if not command else "... ")
            command += line
            if command.lower().strip() == 'exit':
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
