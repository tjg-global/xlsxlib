import os, sys

import xlsxlib


class Database():

    pre_amble = ""

    def __init__(self, db):
        self.db = db

    @staticmethod
    def rows(cursor, arraysize=-1):
        """Generate rows using an optional arrayfetch size
        """
        while True:
            remaining_rows = cursor.fetchmany(arraysize)
            if remaining_rows:
                for row in remaining_rows:
                    yield row
            else:
                break

    def pre_query(self):
        if self.pre_amble:
            q = self.db.cursor()
            q.execute(self.pre_amble)
            q.close()


class SQL_server(Database):

    pre_amble = '''
    SET NOCOUNT ON;
    SET ANSI_WARNINGS ON;
    SET ANSI_NULLS ON;   
    '''

    def cursor_data(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)

        more_data = True
        while more_data:
            # Skip over non-DQL
            while more_data and not cursor.description:
                more_data = cursor.nextset()

            # Fetch sheet name as single-line query
            if more_data:
                sheet_name = cursor.fetchone()[0]
                more_data = cursor.nextset()

            # Skip over non-DQL
            while more_data and not cursor.description:
                more_data = cursor.nextset()

            if more_data:
                yield sheet_name, [d[0:2] for d in cursor.description], self.rows(cursor, 1000)
                more_data = cursor.nextset()


class Snowflake(Database):

    pre_amble = "USE ROLE DEV_ENGINEER"

    @staticmethod
    def nextset(query_iter, cursor):
        try:
            next_query = next(query_iter)
            cursor.execute(next_query)
            return True
        except StopIteration:
            return False

    def cursor_data(self, query):
        queries = [i.strip() for i in query.split(";") if i.strip()]
        query_iter = iter(queries)
        cursor = self.db.cursor()
        more_data = True

        while more_data:
            while more_data and not cursor.description:
                more_data = self.nextset(query_iter, cursor)

            if more_data:
                sheet_name = cursor.fetchone()[0]
                more_data = self.nextset(query_iter, cursor)

            while more_data and not cursor.description:
                more_data = self.nextset(query_iter, cursor)

            if more_data:
                yield sheet_name, [d[0:2] for d in cursor.description], self.rows(cursor, 1000)
                more_data = self.nextset(query_iter, cursor)

class sqlite(Database):

    @staticmethod
    def nextset(query_iter, cursor):
        try:
            next_query = next(query_iter)
            cursor.execute(next_query)
            return True
        except StopIteration:
            return False


    def cursor_data(self, query):
        queries = [i.strip() for i in query.split(";") if i.strip()]
        query_iter = iter(queries)
        cursor = self.db.cursor()
        more_data = True

        while more_data:
            while more_data and not cursor.description:
                more_data = self.nextset(query_iter, cursor)

            if more_data:
                sheet_name = cursor.fetchone()[0]
                more_data = self.nextset(query_iter, cursor)

            while more_data and not cursor.description:
                more_data = self.nextset(query_iter, cursor)

            if more_data:
                yield sheet_name, [d[0:2] for d in cursor.description], self.rows(cursor, 1000)
                more_data = self.nextset(query_iter, cursor)

