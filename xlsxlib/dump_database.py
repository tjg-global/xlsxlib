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

PREAMBLE = r'(?:create or replace)(?:transient)?\s+(database|table|schema|sequence|task|view|materialized view)\s+([0-9A-Za-z_.$"]+)'
#~ LEADING_WORDS = set("create or replace transient".split())
#~ TYPE_NAMES = set(["database", "table", "schema", "sequence", "table", "task", "view", "materialized view"])
def dump_database(database_name, text, debug=False):
    """Take the text of a database GET_DDL and split into component objects

    NB initially the approach was to create a folder for each database and, within
    that, one folder for each type. This was changed to have just a single set of
    type folders, but the database name has been retained in case it's useful to
    return to that approach
    """
    print("\nDatabase:", database_name)

    #
    # Break the main DDL out into its component objects, each one starting
    # with "CREATE OR REPLACE" and ending with a semicolon
    #
    #~ r = re.compile(r'(?:create or replace)(?:transient)?\s+(table|view|materialized view)\s+([0-9A-Za-z_."]+)')
    r1 = re.compile(r"create or replace[^;]*;", flags=re.DOTALL)
    r_preamble = re.compile(PREAMBLE)

    #
    # Within each object definition, skip to the object type / object name
    # and use those to form the relevant folder and file names
    #
    already_seen = set()
    for obj in r1.findall(text):
        print(obj)
        type, name = r_preamble.match(obj.lower()).groups()
        name = name.replace('"', '')
        #~ words = iter(re.findall('[0-9A-Za-z_."]+', obj.lower()))
        #~ remaining_words = itertools.dropwhile(lambda x: x in LEADING_WORDS, words)

        #~ type = next(remaining_words)
        #~ name = next(remaining_words).replace('"', '')
        print(type, "=>", name)
        if (type, name) in already_seen:
            raise RuntimeError("Already seen this combination: %s/%s" % (type, name))
        else:
            already_seen.add((type, name))

        type_dirpath = os.path.join(type.lower())
        if not os.path.exists(type_dirpath):
            os.mkdir(type_dirpath)

        with open(os.path.join(type_dirpath, "%s.sql" % (name)), "w") as f:
            f.write(obj)

if __name__ == '__main__':
    from_filepath(*sys.argv[1:])

