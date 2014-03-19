import logging
import resource

from deimos.logger import log
from deimos._struct import _Struct


def report(level=logging.DEBUG):
    self(level)
    children(level)

def self(level=logging.DEBUG):
    log.log(level, rusage(resource.RUSAGE_SELF))

def children(level=logging.DEBUG):
    log.log(level, rusage(resource.RUSAGE_CHILDREN))

def rusage(target=resource.RUSAGE_SELF):
    r = resource.getrusage(target)
    fmt = "rss = %0.03fM  user = %0.03f  sys = %0.03f"
    return fmt % (r.ru_maxrss / (1024.0 * 1024.0), r.ru_utime, r.ru_stime)

