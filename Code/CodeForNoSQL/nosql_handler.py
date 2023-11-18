from CodeForNoSQL.CodeForNoSQL import Database,  QueryParser

class NoSQLHandler:
    def __init__(self, db_path='../NoSQL_DB/'):
        self.db_path = db_path
        self.database = Database(db_path)


    def handle_commands(self):
        print("NoSQL Handler active. Type 'exit' to return to the main menu.")
        command = ""
        while True:
            line = input("NoSQL_DB > " if not command else "... ")
            command += line
            if command.lower().strip() == 'exit':
                return
            if command.endswith(';'):
                command = command[:-1].strip()  # Remove the semicolon and strip whitespace
                self.database.query(command)(command)
                command = ""  # Reset command for the next input

