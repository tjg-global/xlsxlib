"""Take a Snowflake GET_DDL dump of a database and split into its component objects

If you run GET_DDL on a Snowflake database it will return a series of object DDL
leading with CREATE OR REPLACE [TRANSIENT] <object type> <object name>.

There's a need to break those objects out into individual files and put all
tables in one folder, all views in another etc.

This can be run in two ways: from a text file containing the results of a GET_DDL
command; or from the text of the GET_DDL command directly.
"""
import os, sys
import itertools
import re

def from_filepath(filepath):
    """Assume the database name from the file and extract the text
    """
    database_name, _ = os.path.splitext(os.path.basename(filepath))
    with open(filepath) as f:
        text = f.read()
    dump_database(database_name, text)

R_PREAMBLE = re.compile(r'(?:create or replace)\s*(?:transient)?\s+(database|table|schema|sequence|task|view|materialized view)\s+([0-9A-Za-z_.$"]+)')
def dump_database(database_name, text, debug=False):
    """Take the text of a database GET_DDL and split into component objects

    NB initially the approach was to create a folder for each database and, within
    that, one folder for each type. This was changed to have just a single set of
    type folders, but the database name has been retained in case it's useful to
    return to that approach
    """
    #
    # Break the main DDL out into its component objects, each one starting
    # with "CREATE OR REPLACE" and ending with a semicolon
    #
    r1 = re.compile(r"create or replace[^;]*;", flags=re.DOTALL)

    #
    # Within each object definition, skip to the object type / object name
    # and use those to form the relevant folder and file names
    #
    already_seen = set()
    for obj in r1.findall(text):
        matched = R_PREAMBLE.match(obj.lower())
        if not matched:
            print(obj)
            raise RuntimeError("Unable to match type, name")

        type, name = matched.groups()
        name = name.replace('"', '')
        print(type, "=>", name)
        if (type, name) in already_seen:
            raise RuntimeError("Already seen this combination: %s/%s" % (type, name))
        else:
            already_seen.add((type, name))

        type_dirpath = os.path.join(type.lower())
        if not os.path.exists(type_dirpath):
            os.mkdir(type_dirpath)

        filename = re.sub(r"[ ,.<>\"]", "_", name)
        with open(os.path.join(type_dirpath, "%s.sql" % filename), "w") as f:
            f.write(obj)

def dump_imported_database(database_name, debug=False):
    type_dirpath = "database"
    if not os.path.exists(type_dirpath):
        os.mkdir(type_dirpath)

    filename = re.sub(r"[ ,.<>\"]", "_", database_name)
    with open(os.path.join(type_dirpath, "%s.sql" % filename), "w") as f:
        f.write(f"""create or replace database {database_name}:
-- This is an 'Imported Database'
-- The "GET_DDL" function has not extracted any objects within this database
""")

if __name__ == '__main__':
    from_filepath(*sys.argv[1:])

