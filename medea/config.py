from ConfigParser import SafeConfigParser, NoSectionError
import json
import logging
import os
import sys

import medea.docker
from medea.logger import log
import medea.logger
from medea._struct import _Struct


def load_configuration(f=None, interactive=sys.stdout.isatty()):
    logconf, docker, containers = Log(), Docker(), Containers()
    error = None
    logconf.console = logging.DEBUG if interactive     else None
    logconf.syslog  = logging.INFO  if not interactive else None
    try:
        f = f if f else path()
        if f:
            logconf, docker, containers = parse(f)
    except Exception as e:
        error = e
    finally:
        medea.logger.initialize(**dict(logconf.items(onlyset=True)))
        if error:
            log.exception((("Error loading %s: " % f) if f else "")+str(error))
            sys.exit(16)
        if f:
            log.info("Loaded configuration from %s" % f)
    return logconf, docker, containers

def coercearray(array):
    if array[0:1] != "[":
        return [array]
    try:
        arr = json.loads(array)
        if type(arr) is not list:
            raise ValueError() 
        return arr
    except:
        raise ValueError("Not an array: %s" % array)

def coerceloglevel(level):
    if not level:
        return
    if type(level) is int:
        return level
    levels = { "DEBUG"    : logging.DEBUG,
               "INFO"     : logging.INFO,
               "WARNING"  : logging.WARNING,
               "ERROR"    : logging.ERROR,
               "CRITICAL" : logging.CRITICAL,
               "NOTSET"   : logging.NOTSET }
    try:
        return levels[level]
    except:
        raise ValueError("Not a log level: %s" % level)

def coercebool(b):
    if type(b) is bool:
        return b
    try:
        bl = json.loads(b)
        if type(bl) is not bool:
            raise ValueError() 
        return bl
    except:
        raise ValueError("Not a bool: %s" % b)
class Image(_Struct):
    def __init__(self, default=None, ignore=False):
        _Struct.__init__(self, default=default, ignore=coercebool(ignore))


class Options(_Struct):
    def __init__(self, default=[], append=[], ignore=False):
        _Struct.__init__(self, default=coercearray(default),
                               append=coercearray(append),
                               ignore=coercebool(ignore))

class Containers(_Struct):
    def __init__(self, image=Image(), options=Options()):
        _Struct.__init__(self, image=image, options=options)

class Log(_Struct):
    def __init__(self, console=None, syslog=None):
        _Struct.__init__(self, console=coerceloglevel(console),
                               syslog=coerceloglevel(syslog))

class Docker(_Struct):
    def __init__(self, **properties):
        for k in properties.keys():
            properties[k] = coercearray(properties[k])
        _Struct.__init__(self, **properties)
    def argv(self):
        return [ [("--" + k, v) for v in arr] for k, arr in self.items() ]


def parse(f):
    config = SafeConfigParser()
    config.read(f)
    try:
        log = Log(**dict(config.items("log")))
    except NoSectionError:
        log = Log()
    try:
        image = Image(**dict(config.items("containers.image")))
    except NoSectionError:
        image = Image()
    try:
        options = Options(**dict(config.items("containers.options")))
    except NoSectionError:
        options = Options()
    try:
        docker = Docker(**dict(config.items("docker")))
    except NoSectionError:
        docker = Docker()
    return (log, docker, Containers(image, options))

def path():
    for p in search_path:
        if os.path.exists(p):
            return p

search_path = ["./medea.cfg",
               os.path.expanduser("~/.medea"),
               "/etc/medea.cfg",
               "/usr/etc/medea.cfg",
               "/usr/local/etc/medea.cfg"]

