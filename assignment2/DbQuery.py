from DbConnector import DbConnector
from tabulate import tabulate
import mysql.connector as mysql
import csv


class QueryClass:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name, property):
        columns=''
        for col in property:
            temp=col+" "+property[col]+','
            columns+=temp
        query = """CREATE TABLE IF NOT EXISTS %s (%s)"""
        #This adds table_name to the %s variable and executes the query
        #print(query % (table_name,columns[:-1]))
        self.cursor.execute(query % (table_name,columns[:-1]))
        self.db_connection.commit()

    def insert_data(self, query,data):
        try:
            self.cursor.executemany(query, data)
            self.db_connection.commit()
            print(self.cursor.rowcount,"records inserted!")
        except:
            for d in data:
                if len(d)<5:
                    print(d)
        

    def read_data(self,query):
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        return records

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
    def delete_table(self,tables):
        for table in tables:
            query = "DROP TABLE %s"
            self.cursor.execute(query % table)


# def main():
#     program = None
    
#     try:
#         program = QueryClass()
#         program.create_table(table_name="Person")
#         program.insert_data(table_name="Person")
#         _ = program.fetch_data(table_name="Person")
#         program.show_tables()
#         program.drop_table(table_name="Person")
#         # Check that the table is dropped
#         program.show_tables()
#     except Exception as e:
#         print("ERROR: Failed to use database:", e)
#     finally:
#         if program:
#             program.connection.close_connection()


# if __name__ == '__main__':
#     main()
