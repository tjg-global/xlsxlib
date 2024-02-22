import os, sys
import re

class Database():

    preamble = ""

    def __init__(self, db):
        self.db = db
        self.preamble = self.__class__.preamble

    @staticmethod
    def nextset(query_iter, cursor):
        try:
            next_query = next(query_iter)
            cursor.execute(next_query)
            return True
        except StopIteration:
            return False

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

    @staticmethod
    def from_code(code):
        return " ".join(code.split("_")).title()

    def replace_variables(self, query, params):
        vars = re.findall(r"%\((\w+)\)s", query)
        values = {}
        for index, var in enumerate(vars):
            try:
                values[var] = params[index]
            except IndexError:
                if var not in values:
                    values[var] = input("%s: " % self.from_code(var))
        if values:
            query = query % values

        return query

    def preprocess(self, query, params):
        return self.replace_variables(query, params)

    def pre_query(self):
        if self.preamble:
            q = self.db.cursor()
            try:
                q.execute(self.preamble)
            finally:
                q.close()


class SQLServer(Database):

    preamble = '''
    SET NOCOUNT ON;
    SET ANSI_WARNINGS ON;
    SET ANSI_NULLS ON;
    '''

    def preprocess(self, query, params):
        query = re.sub(r"USE\s+.*", "", query)
        query = re.sub(r"\bGO\b", "", query)
        query = self.replace_variables(query, params)
        return query

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

    preamble = ""

    def pre_query(self):
        if self.preamble:
            q = self.db.cursor()
            try:
                for statement in self.preamble.split(";"):
                    q.execute(statement)
            finally:
                q.close()

    def preprocess(self, query, params):
        #
        # Find any instance of a USE DATABASE etc. command
        # Add it to the preamble and then remove from the query
        # NB this is very naive, assuming that all the USE statements
        # are at the top of the file. If any is effectively between
        # different statements then this will fail to achieve the
        # desired effect
        #
        r_use_statement = re.compile(
            r"USE\s+(?:DATABASE|ROLE|WAREHOUSE|SCHEMA)\s+\w+;",
            flags=re.IGNORECASE
        )
        use_statements = r_use_statement.findall(query)
        self.preamble += "\n".join(use_statements)
        query = r_use_statement.sub("", query)

        query = self.replace_variables(query, params)

        return query

    def cursor_data(self, query):
        queries = [i.strip() for i in query.split(";") if i.strip()]
        query_iter = iter(queries)
        cursor = self.db.cursor()
        more_data = True

        while more_data:
            while more_data and not cursor.rowcount:
                more_data = self.nextset(query_iter, cursor)

            if more_data:
                sheet_name = cursor.fetchone()[0]
                more_data = self.nextset(query_iter, cursor)

            while more_data and not cursor.rowcount:
                more_data = self.nextset(query_iter, cursor)

            if more_data:
                yield sheet_name, [d[0:2] for d in cursor.description], self.rows(cursor, 1000)
                more_data = self.nextset(query_iter, cursor)

class sqlite(Database):

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

_DRIVER_DIALECTS = {
    "mssql" : SQLServer,
    "snowflake" : Snowflake,
    "sqlite" : sqlite
}
def dialect_from_driver(driver):
    return _DRIVER_DIALECTS[driver]