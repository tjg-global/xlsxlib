import os, sys
import re

def main(filepath):
    db_name, _ = os.path.splitext(os.path.basename(filepath))
    print("Database:", db_name)
    print(os.getcwd())
    if not os.path.exists(db_name):
        os.mkdir(db_name)

    with open(filepath) as f:
        text = f.read()

    r1 = re.compile(r"create or replace[^;]*;", flags=re.DOTALL)
    for obj in r1.findall(text):
        line1 = obj.splitlines()[0].strip("(;")
        line1 = line1[len("create or replace "):]
        type, name = line1.split()
        print(type, "=>", name)

        type_dirpath = os.path.join(db_name, type.lower())
        if not os.path.exists(type_dirpath):
            os.mkdir(type_dirpath)

        with open(os.path.join(type_dirpath, "%s.sql" % (name)), "w") as f:
            f.write(obj)

if __name__ == '__main__':
    main(*sys.argv[1:])

