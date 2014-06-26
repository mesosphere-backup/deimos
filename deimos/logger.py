import inspect
import logging
import logging.handlers
import os


root = logging.getLogger("deimos")


class log():  # Really just a namespace

    @staticmethod
    def debug(*args, **opts):
        logger(2).debug(*args, **opts)

    @staticmethod
    def info(*args, **opts):
        logger(2).info(*args, **opts)

    @staticmethod
    def warning(*args, **opts):
        logger(2).warning(*args, **opts)

    @staticmethod
    def error(*args, **opts):
        logger(2).error(*args, **opts)

    @staticmethod
    def critical(*args, **opts):
        logger(2).critical(*args, **opts)

    @staticmethod
    def exception(*args, **opts):
        logger(2).exception(*args, **opts)

    @staticmethod
    def log(*args, **opts):
        logger(2).log(*args, **opts)


def initialize(console=logging.DEBUG, syslog=logging.INFO):
    global _settings
    global _initialized
    if _initialized:
        return
    _settings = locals()
    _initialized = True
    root.setLevel(min(level for level in [console, syslog] if level))
    if console:
        stderr = logging.StreamHandler()
        fmt = "%(asctime)s.%(msecs)03d %(name)s %(message)s"
        stderr.setFormatter(logging.Formatter(fmt=fmt, datefmt="%H:%M:%S"))
        stderr.setLevel(console)
        root.addHandler(stderr)
    if syslog:
        dev = "/dev/log" if os.path.exists("/dev/log") else "/var/run/syslog"
        fmt = "deimos[%(process)d]: %(name)s %(message)s"
        logger = logging.handlers.SysLogHandler(address=dev)
        logger.setFormatter(logging.Formatter(fmt=fmt))
        logger.setLevel(syslog)
        root.addHandler(logger)
    root.removeHandler(_null_handler)


def logger(height=1):                 # http://stackoverflow.com/a/900404/48251
    """
    Obtain a function logger for the calling function. Uses the inspect module
    to find the name of the calling function and its position in the module
    hierarchy. With the optional height argument, logs for caller's caller, and
    so forth.
    """
    caller = inspect.stack()[height]
    scope = caller[0].f_globals
    function = caller[3]
    path = scope["__name__"]
    if path == "__main__" and scope["__package__"]:
        path = scope["__package__"]
    return logging.getLogger(path + "." + function + "()")

_initialized = False

_settings = {}

_null_handler = logging.NullHandler()

root.addHandler(_null_handler)
