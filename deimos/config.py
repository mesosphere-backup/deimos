from ConfigParser import SafeConfigParser, NoSectionError
import json
import logging
import os
import sys

import deimos.argv
import deimos.docker
from deimos.logger import log
import deimos.logger
from deimos._struct import _Struct


def load_configuration(f=None, interactive=sys.stdout.isatty()):
    error = None
    defaults = _Struct(docker=Docker(),
                       index=DockerIndex(),
                       containers=Containers(),
                       uris=URIs(),
                       state=State(),
                       hooks=Hooks(),
                       log=Log(
                       console=(logging.DEBUG if interactive else None),
                       syslog=(logging.INFO if not interactive else None)
                       ))
    parsed = None
    try:
        f = f if f else path()
        if f:
            parsed = parse(f)
    except Exception as e:
        error = e
    finally:
        confs = defaults.merge(parsed) if parsed else defaults
        deimos.logger.initialize(**dict(confs.log.items()))
        if error:
            pre = ("Error loading %s: " % f) if f else ""
            log.exception(pre + str(error))
            sys.exit(16)
        if parsed:
            log.info("Loaded configuration from %s" % f)
            for _, conf in parsed.items():
                log.debug("Found: %r", conf)
    return confs


def coercearray(array):
    if type(array) in deimos.argv.strings:
        if array[0:1] != "[":
            return [array]
        try:
            arr = json.loads(array)
            if type(arr) is not list:
                raise ValueError()
            return arr
        except:
            raise ValueError("Not an array: %s" % array)
    return list(array)


def coerceloglevel(level):
    if not level:
        return
    if type(level) is int:
        return level
    levels = {"DEBUG": logging.DEBUG,
              "INFO": logging.INFO,
              "WARNING": logging.WARNING,
              "ERROR": logging.ERROR,
              "CRITICAL": logging.CRITICAL,
              "NOTSET": logging.NOTSET}
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


def coerceoption(val):
    try:
        return coercearray(val)
    except:
        return coercebool(val)


class Image(_Struct):

    def __init__(self, default=None, ignore=False):
        _Struct.__init__(self, default=default, ignore=coercebool(ignore))

    def override(self, image=None):
        return image if (image and not self.ignore) else self.default


class Hooks(_Struct):

    def __init__(self, unpack=False, onlaunch=[], ondestroy=[]):
        _Struct.__init__(self, onlaunch=coercearray(onlaunch),
                               ondestroy=coercearray(ondestroy))

    def override(self, options=[]):
        return self.onlaunch.override(onlaunch), self.ondestroy.override(ondestroy)


class Options(_Struct):

    def __init__(self, default=[], append=[], ignore=False):
        _Struct.__init__(self, default=coercearray(default),
                               append=coercearray(append),
                               ignore=coercebool(ignore))

    def override(self, options=[]):
        a = options if (len(options) > 0 and not self.ignore) else self.default
        return a + self.append


class Containers(_Struct):

    def __init__(self, image=Image(), options=Options()):
        _Struct.__init__(self, image=image, options=options)

    def override(self, image=None, options=[]):
        return self.image.override(image), self.options.override(options)


class URIs(_Struct):

    def __init__(self, unpack=True):
        _Struct.__init__(self, unpack=coercebool(unpack))


class Log(_Struct):

    def __init__(self, console=None, syslog=None):
        _Struct.__init__(self, console=coerceloglevel(console),
                               syslog=coerceloglevel(syslog))


class Docker(_Struct):

    def __init__(self, **properties):
        for k in properties.keys():
            properties[k] = coerceoption(properties[k])
        _Struct.__init__(self, **properties)

    def argv(self):
        return deimos.argv.argv(**dict(self.items()))


class DockerIndex(_Struct):

    def __init__(self, index=None, account_libmesos="libmesos",
                                   account=None,
                                   dockercfg=None):
        _Struct.__init__(self, index=index,
                               account_libmesos=account_libmesos,
                               account=account,
                               dockercfg=dockercfg)


class State(_Struct):

    def __init__(self, root="/tmp/deimos"):
        if ":" in root:
            raise ValueError("Deimos root storage path must not contain ':'")
        _Struct.__init__(self, root=root)


def parse(f):
    config = SafeConfigParser()
    config.read(f)
    parsed = {}
    sections = [("log", Log), ("state", State), ("uris", URIs),
                ("docker", Docker),
                ("docker.index", DockerIndex),
                ("containers.image", Image),
                ("hooks", Hooks),
                ("containers.options", Options)]
    for key, cls in sections:
        try:
            parsed[key] = cls(**dict(config.items(key)))
        except:
            continue
    containers = {}
    if "containers.image" in parsed:
        containers["image"] = parsed["containers.image"]
        del parsed["containers.image"]
    if "containers.options" in parsed:
        containers["options"] = parsed["containers.options"]
        del parsed["containers.options"]
    if len(containers) > 0:
        parsed["containers"] = Containers(**containers)
    if "docker.index" in parsed:
        parsed["index"] = parsed["docker.index"]
        del parsed["docker.index"]
    return _Struct(**parsed)


def path():
    for p in search_path:
        if os.path.exists(p):
            return p

search_path = ["./deimos.cfg",
               os.path.expanduser("~/.deimos"),
               "/etc/deimos.cfg",
               "/usr/etc/deimos.cfg",
               "/usr/local/etc/deimos.cfg"]
