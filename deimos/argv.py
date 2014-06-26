# coding: utf-8


def argv(*args, **opts):
    """
    Produces an argument vector from its array of arguments and keyword
    options. First, the options are unpacked. When the value is a ``bool``,
    the option is passed without an argument if it is true and skipped if it
    is ``false``. When the option is one of the flat built-in types -- a
    ``string`` or ``unicode`` or ``bytes`` or an ``int`` or a ``long`` or a
    ``float`` -- it is passed literally. If the value is a subclass of
    ``dict``, ``.items()`` is called on it the option is passed multiple times
    for key-value pair, with the key and value joined by an ``=``. Otherwise,
    if the value is iterable, the option is passed once for each element, and
    each element is treated like an atomic type. Underscores in the names of
    options are turned in to dashes. If the name of an option is a single
    letter, only a single dash is used when passing it. If an option is passed
    with the key ``__`` and value ``True``, it is put at the end of the
    argument list. The arguments are appended to the end of the argument list,
    each on treated as an atomic type.

    >>> argv.argv(1, 2, 'a', u'ü', dev='/dev/cba', v=True, y=[3,2])
    ['-y', '3', '-y', '2', '--dev', '/dev/cba', '-v', '1', '2', 'a', u'\xfc']

    """
    spacer = ["--"] if opts.get("__") else []
    args = [arg(_) for _ in args]
    opts = [_ for k, v in opts.items() for _ in opt(k, v)]
    return opts + spacer + args


def arg(v):
    if type(v) in strings:
        return v
    if type(v) in nums:
        return str(v)
    raise TypeError("Type %s is not a simple, flat type" % type(v))


def opt(k, v):
    k = arg(k).replace("_", "-")
    if k == "--":
        return []
    k = ("--" if len(k) > 1 else "-") + k
    if type(v) is bool:
        return [k] if v else []
    if type(v) in simple:
        return [k, arg(v)]
    if isinstance(v, dict):
        v = ["%s=%s" % (arg(kk), arg(vv)) for kk, vv in v.items()]
    return [_ for element in v for _ in [k, arg(element)]]


nums = set([int, long, float])
strings = set([str, unicode, bytes])
simple = strings | nums
