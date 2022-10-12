def _set (obj, attr, value):
  obj.__dict__[attr] = value

class _Row (object):

  def __init__ (self, row):
    _set (self, "row", list (row))

  def __getitem__ (self, index):
    if isinstance (index, int):
      return self.row[index]
    else:
      return self.row[self.description[index]]

  def __setitem__ (self, index, value):
    if isinstance (index, int):
      self.row[index] = value
    else:
      self.row[self.description[index]] = value

  def __getattr__ (self, key):
    try:
      return self.row[self.description[key]]
    except KeyError:
      raise AttributeError

  def __setattr__ (self, key, value):
    self.row[self.description[key]] = value

  def __repr__ (self):
    return "<Row: %s>" % self.as_string ()

  def __str__ (self):
    return self.as_string ()

  def __setstate__ (self, pickled):
    self.__dict__.update (pickled)

  def __len__ (self):
    return len (self.row)

  def __nonzero__ (self):
    return bool (self.row)

  def as_tuple (self):
    return tuple (self.row)

  def as_dict (self):
    return dict ((name, self.row[index]) for name, index in self.description.items ())

  def as_string (self):
    return str (self.as_tuple ())

Rows = {}
def Row (names):
  code = " ".join (name.lower () for name in names)
  if code not in Rows:
    Rows[code] = type (
      code.encode ("utf8"),
      (_Row,),
      dict (
        columns = names,
        description = dict ((name, index) for index, name in enumerate (names))
      )
    )
  return Rows[code]

def row (record):
  return Row ([i[0] for i in record.cursor_description]) (record)

def from_dict (dict):
  items = dict.items ()
  return Row ([k for k, v in items]) (v for k, v in items)
