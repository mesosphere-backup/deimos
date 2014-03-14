import os
import signal

import deimos.logger


def is_signal_name(s):
    return s.startswith("SIG") and not s.startswith("SIG_")

names = dict((getattr(signal, s), s) for s in dir(signal) if is_signal_name(s))

def install(f, signals=[signal.SIGINT, signal.SIGTERM]):
    log = deimos.logger.logger(2)
    def handler(signum, _):
        log.warning("%s (%d)", names.get(signum, "SIG???"), signum)
        response = f(signum)
        if type(response) == Resume:
            return
        if type(response) is int:
            os._exit(response)
        os._exit(-signum)
    for _ in signals: signal.signal(_, handler)

class Resume(object):
    def __eq__(self, other):
        return self.__class__ == other.__class__

