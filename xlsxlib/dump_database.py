"""Take a Snowflake GET_DDL dump of a database and split into its component objects

If you run GET_DDL on a Snowflake database it will return a series of object DDL
leading with CREATE OR REPLACE [TRANSIENT] <object type> <object name>.

There's a need to break those objects out into individual files and put all
tables in one folder, all views in another etc.

This can be run in two ways: from a text file containing the results of a GET_DDL
command; or from the text of the GET_DDL command directly.
"""
import os, sys
import glob
import itertools
import logging
from pathlib import Path
import re
import shutil

TYPES = {
    "database", "table", "schema", "sequence", "task", "view",
    "materialized view", "dynamic table", "stream", "pipe", "secure view",
    "tag", "file format", "function", "procedure", "temporary table",
    "alert", "iceberg table", "streamlit", "event table"
}

def from_filepath(filepath):
    """Assume the database name from the file and extract the text
    """
    database_name, _ = os.path.splitext(os.path.basename(filepath))
    with open(filepath) as f:
        text = f.read()
    dump_database(database_name, text)

def munged_name(name):
    """Remove characters from a database object name which aren't valid on the filesystem
    """
    return re.sub(r"[ ,<>\"]", "_", name)

def remove_existing_files(database_name=None, logger=logging):
    """Remove the files for one or all databases

    The code is maintaining a git-ready copy of the database structures so that
    changes can be seen. To track removed (or renamed) objects, including databases
    we need to remove them before rebuilding. To track entirely removed databases
    we need to remove everything before starting.

    The code is kept here as this module knows about the file structure it's writing
    into, while the dump_snowflake_databases wrapper only knows about the databases
    it's reading and hands off to this module for the filesystem.
    """
    if database_name:
        for type in TYPES:
            #
            # Remove object definition files for this database
            #
            for filepath in glob.glob(os.path.join(type, "%s.sql" % database_name)):
                os.unlink(filepath)
            #
            # Remove the database definition itself
            #
            for filepath in glob.glob(os.path.join(type, "%s.*.sql" % database_name)):
                os.unlink(filepath)

    else:
        #
        # Remove any non-dot directories: they'll be recreated on demand by the
        # dump_database functionality below. (NB this will work whether we're
        # expecting to find type-specific folders or database-specific folders)
        #
        logger.info("Removing files for all databases")
        candidate_dirs = [f for f in Path(".").iterdir() if f.is_dir() and not f.match(".*")]
        for dirpath in candidate_dirs:
            shutil.rmtree(dirpath)

R_PREAMBLE = re.compile(
    r'(?:create or replace)\s*(?:transient)?\s+(%s)\s+([0-9A-Za-z_.$\-"]+)' % "|".join(TYPES),
    flags=re.IGNORECASE
)
def dump_database(database_name, text, debug=False, logger=logging):
    """Take the text of a database GET_DDL and split into component objects

    NB initially the approach was to create a folder for each database and, within
    that, one folder for each type. This was changed to have just a single set of
    type folders, but the database name has been retained in case it's useful to
    return to that approach
    """
    #
    # Remove any existing object definitions for this database
    # from each type folder
    #
    remove_existing_files(database_name, logger)

    #
    # Replace windows-style CR/LF line feeds with unix-style LF only
    #
    text = text.replace("\r", "") + "\n"

    #
    # Break the main DDL out into its component objects, each one starting
    # with "CREATE OR REPLACE" and ending with a semicolon.
    # FIXME: this won't actually work successfully for, eg, functions & procedures
    # which can have embedded semicolons. But it'll do for now
    #
    r_creates = re.compile(r"create or replace", flags=re.IGNORECASE)
    positions = [i.span() for i in r_creates.finditer(text)]
    spans = [(p[0], q[0]) for (p, q) in zip(positions, positions[1:])] + [(positions[-1][0], len(text))]

    objects = [text[i:j] for (i, j) in spans]
    for obj in objects:
        #
        # Extract the object type & name from the object definition
        # and use these to determine the folder and file name to use.
        #
        matched = R_PREAMBLE.match(obj)
        if not matched:
            logger.error("Unable to match type, name from:\n%s", obj)
            continue

        type, name = matched.groups()
        type = type.lower()
        #
        # Strip off any leading/trailing double-quotes
        # Any embedded ones will be picked up by the "munged_name" logic
        #
        name = name.replace('"', '')
        #
        # The object type will determine the folder to be used. If the
        # corresponding folder doesn't already exist, create it
        #
        type_dirpath = os.path.join(type)
        if not os.path.exists(type_dirpath):
            os.mkdir(type_dirpath)

        logger.debug("%s => %s", type, name)

        #
        # If the db object name has characters which won't be valid on
        # a filesystem, replace them with underscores
        #
        filename = munged_name(name)
        #
        # We have some databases where two versions of the same object
        # exist, differing only by case. On Windows at least, the
        # filesystem won't recognise them as different, so artifically
        # add a suffix as needed
        #
        while True:
            filepath = os.path.join(type_dirpath, "%s.sql" % filename)
            if not os.path.exists(filepath):
                break
            logger.warn("%s already exists; adding suffix", filename)
            filename += "_"

        #
        # Write the object definition to the (if necessary) munged filename
        # in the type-specific folder
        #
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(obj)

def dump_imported_database(database_name, debug=False, logger=logging):
    """Write a placeholder for an imported database where we don't have the definitions

    Imported databases are links to databases in other Snowflake instances. We have
    rights to access the database, but not its definition.
    """
    type_dirpath = "database"
    if not os.path.exists(type_dirpath):
        os.mkdir(type_dirpath)

    filename = munged_name(database_name)
    with open(os.path.join(type_dirpath, "%s.sql" % filename), "w", encoding="utf-8") as f:
        f.write(f"""create or replace database {database_name}:
-- This is an 'Imported Database'
-- The "GET_DDL" function has not extracted any objects within this database
""")

if __name__ == '__main__':
    from_filepath(*sys.argv[1:])

