"""Take a Snowflake GET_DDL dump of a database and split into its component objects
fr
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
import re
import shutil

import sqlglot


TYPES = {
    "database" : "non-programmatic",
    "schema" : "non-programmatic",
    "table" : "non-programmatic",
    "temporary table" : "non-programmatic",
    "dynamic table" : "non-programmatic",
    "iceberg table" : "non-programmatic",
    "event table" : "non-programmatic",
    "view" : "non-programmatic",
    "materialized view" : "non-programmatic",
    "secure view" : "non-programmatic",
    "sequence" : "non-programmatic",

    "task" : "programmatic",
    "stream" : "programmatic",
    "pipe" : "programmatic",
    "tag" : "programmatic",
    "file format" : "programmatic",
    "function" : "programmatic",
    "procedure" : "programmatic",
    "alert" : "programmatic",
    "streamlit" : "programmatic"
}
DATATYPE_SHORT_NAMES = {
    "TIMESTAMPNTZ" : "NTZ",
    "TIMESTAMPLTZ" : "LTZ",
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
    return re.sub(r"[<>\"]", "_", name)

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
        # Remove all directories: they'll be recreated on demand by the
        # dump_database functionality below. (NB this will work whether we're
        # expecting to find type-specific folders or database-specific folders)
        #
        logger.info("Removing files for all databases")
        for dirpath in os.listdir("."):
            if dirpath.startswith("."):
                continue
            if os.path.isdir(dirpath):
                shutil.rmtree(dirpath)

def chunks_from_pattern(pattern, text):
    """Use pattern as a delimiter within text

    Return an iterable of each chunk of `text` which starts with `pattern`
    """
    r = re.compile(pattern, flags=re.IGNORECASE)
    positions = [i.span() for i in r.finditer(text)]
    spans = [(p[0], q[0]) for (p, q) in zip(positions, positions[1:])] + [(positions[-1][0], len(text))]
    for i, j in spans:
        yield text[i:j]

def comments_removed(text):
    """Take a block of SQL and remove inline (--) and block (/* */) comments

    This is so that we can test for mismatched quote marks without hitting
    "word" quotes inside comment blocks (eg doesn't; isn't)
    """
    #
    # Hack to work around a specific piece of code where we compare
    # against the string '---' (which confuses our comment/quote-count logic)
    # cf view INTEGRATION.PROCESSED.SOCIALREPORTING_DATA
    #
    text = re.sub(r"'-+'", "", text)
    #
    # We have at least one instance where the code includes an escaped quote
    # (ie '...\' ...') which is perfectly legit. To keep this consistent we
    # can translate that into two quotes. We also have instances of escaped
    # backslashes, so we get rid of those first!
    #
    text = text.replace("\\\\", "").replace("\\'", "''")
    #
    # Remove anything on a single line following a double-dash
    # NB we have at least one view which has: WHERE ... != '---'
    #
    text = re.sub("--.*", "", text)
    #
    # Remove anything within block comment markers (/*...*/)
    #
    text = re.sub(r"\/\*[^*]*\*\/", "", text)

    return text

R_PREAMBLE = re.compile(
    r'(?:create or replace)\s*(?:transient)?\s+(%s)\s+([0-9A-Za-z_.$\-"]+)' % "|".join(TYPES),
    flags=re.IGNORECASE
)
R_OBJECTS = re.compile(r"create or replace", flags=re.IGNORECASE)
def dump_schema(schema_sql, logger):
    """Take apart one schema from the generated DDL

    This is useful because the objects are ordered consistently with a schema:
    tables, views, procedures, tasks etc.

    Therefore any "CREATE TABLE" markers after we've started processing procedures
    are clearly internal DDL and can be ignored
    """
    candidates = chunks_from_pattern("create or replace", schema_sql)

    #
    # Objects appear in the DDL in a certain order: non-programmatic objects (such
    # as tables & views) before programmatic ones (such as procedures & tasks)
    # If we see any create statements for non-programmatic objects after we've
    # started processing programmatic ones, then they're internal to a programmatic
    # object and can be skipped
    #
    objects = []
    seen_programmatic = False
    while True:
        try:
            candidate = next(candidates)
        except StopIteration:
            break
        type, name = R_PREAMBLE.match(candidate).groups()
        is_programmatic = TYPES[type.lower()] == "programmatic"
        logger.debug("*** %s %s", "PROGRAMMATIC" if is_programmatic else "NON-PROGRAMMATIC", candidate)

        #
        # If we're seeing a non-programmtic object (eg a table) _after_ we've
        # seen at least one programmatic object (eg a task) then we assume that
        # this is an internal object.
        #
        if seen_programmatic and not is_programmatic:
            logger.warn("Object %s of type %s appears after we've seen programmatic objects; assuming internal", name, type)
            objects[-1] += candidate
        else:
            objects.append(candidate)

        #
        # Track when we've started to see programmatic objects
        #
        if not seen_programmatic and is_programmatic:
            seen_programmatic = True

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
        # If the type is Procedure or Function then use the remainder of the
        # line as the name (including the object definition)
        #
        if type in ("function", "procedure"):
            sqlobj = sqlglot.parse_one(obj)
            procedure_name = sqlobj.find(sqlglot.exp.Dot).name
            param_names = [p.kind.this.name for p in sqlobj.find_all(sqlglot.exp.ColumnDef)]
            #
            # In some outlier cases the combination of param types is so long
            # that the resulting filename is too long! So shorten the params
            #
            name = "%s(%s)" % (procedure_name, ",".join(DATATYPE_SHORT_NAMES.get(p, p[:3]) for p in param_names))

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
    # We have instances where a database tag is applied by means of code like:
    # alter database <db> set tag RD_SNOWFLAKE_USAGE.CURATED.DATABASE_PURPOSE='COD dev database';
    #
    # Ultimately it would be good track tickets, but for now this just complicates parsing
    # so we pull them out
    #
    text = re.sub(r"alter database \w+ set tag.*;", "", text)

    r_schemas = re.compile(r"create or replace schema", flags=re.IGNORECASE)
    schema_positions = [i.span() for i in r_schemas.finditer(text)]
    schema_spans = [(p[0], q[0]) for (p, q) in zip(schema_positions, schema_positions[1:])] + [(schema_positions[-1][0], len(text))]
    for (i, j) in schema_spans:
        dump_schema(text[i:j], logger)

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

