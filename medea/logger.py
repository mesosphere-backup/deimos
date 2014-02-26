import logging
import logging.handlers
import os


root = logging.getLogger("medea")

def initialize(console=True, syslog=False, level=logging.DEBUG):
    global _settings
    global _initialized
    if _initialized: return
    _settings = locals()
    _initialized = True
    root.setLevel(level)
    if console:
        stderr = logging.StreamHandler()
        fmt = "%(asctime)s.%(msecs)03d %(name)s.%(funcName)s %(message)s"
        stderr.setFormatter(logging.Formatter(fmt=fmt, datefmt="%H:%M:%S"))
        root.addHandler(stderr)
    if syslog:
        dev = "/dev/log" if os.path.exists("/dev/log") else "/var/run/syslog"
        fmt = "%(name)s[%(process)d]: %(funcName)s %(message)s"
        syslog = logging.handlers.SysLogHandler(address=dev)
        syslog.setFormatter(logging.Formatter(fmt=fmt))
        root.addHandler(syslog)
    root.removeHandler(_null_handler)

_initialized = False

_settings = {}

_null_handler = logging.NullHandler()

root.addHandler(_null_handler)
