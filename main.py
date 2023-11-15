# from CodeForSQL.sql_handler import SQLHandler
# from CodeForNoSQL.nosql_handler import NoSQLHandler

def main():
    print("Welcome to ourDB. Please choose 'SQL' or 'NoSQL'. Type 'exit' to quit.")

    while True:
        db_type = input("ourDB > ")
        if db_type.lower() == 'exit':
            print("Exiting ourDB. Goodbye!")
            break
        elif db_type.lower() == 'sql':
            print("sql")
            # sql_handler = SQLHandler()
            # sql_handler.handle_commands()
        elif db_type.lower() == 'nosql':
            print("nosql")
            # nosql_handler = NoSQLHandler()
            # nosql_handler.handle_commands()
        else:
            print("Invalid input. Please choose 'SQL' or 'NoSQL'.")

if __name__ == "__main__":
    main()
