import os, sys
import itertools
import re

def from_filepath(filepath):
    database_name, _ = os.path.splitext(os.path.basename(filepath))
    with open(filepath) as f:
        text = f.read()
    dump_database(database_name, text)

LEADING_WORDS = set("create or replace transient".split())
def dump_database(database_name, text):
    print("Database:", database_name)
    print(os.getcwd())

    r1 = re.compile(r"create or replace[^;]*;", flags=re.DOTALL)
    for obj in r1.findall(text):
        line1 = obj.splitlines()[0].lower().strip("(;")
        words = iter(line1.split())
        remaining_words = itertools.dropwhile(lambda x: x in LEADING_WORDS, words)
        type = next(remaining_words)
        name = next(remaining_words)
        print(type, "=>", name)

        type_dirpath = os.path.join(type.lower())
        if not os.path.exists(type_dirpath):
            os.mkdir(type_dirpath)

        with open(os.path.join(type_dirpath, "%s.sql" % (name)), "w") as f:
            f.write(obj)

if __name__ == '__main__':
    from_filepath(*sys.argv[1:])

