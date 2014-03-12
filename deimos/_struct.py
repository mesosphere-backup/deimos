class _Struct(object):
    def __init__(self, **properties):
        self.__dict__.update(properties)
        self._properties = properties.keys()
    def __repr__(self):
        mod, cls = self.__class__.__module__, self.__class__.__name__
        fields = [ "%s=%r" % (k, v) for k, v in self.items() ]
        return mod + "." + cls + "(" + ", ".join(fields) + ")"
    def keys(self):
        return self._properties
    def items(self, onlyset=False):
        vals = [ (k, self.__dict__[k]) for k in self._properties ]
        return [ (k, v) for k, v in vals if v ] if onlyset else vals
    def merge(self, other):
        # NB: Use leftmost constructor, to recheck validity of fields.
        return self.__class__(**dict(self.items() + other.items()))

