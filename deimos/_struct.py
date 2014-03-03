class _Struct(object):
    def __init__(self, **properties):
        self.__dict__.update(properties)
        self._properties = properties.keys()
    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)
    def keys(self):
        return self._properties
    def items(self, onlyset=False):
        vals = [ (k, self.__dict__[k]) for k in self._properties ]
        return [ (k, v) for k, v in vals if v ] if onlyset else vals

