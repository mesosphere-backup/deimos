import logging
import os
import pipes
import subprocess
import sys

import deimos.logger
from deimos.err import *
from deimos._struct import _Struct


class Run(_Struct):

    def __init__(self, log=None, data=False, in_sh=True,
                       close_stdin=True, log_stderr=True,
                       start_level=logging.DEBUG,
                       success_level=logging.DEBUG,
                       error_level=logging.WARNING):
        _Struct.__init__(self, log=(log if log else deimos.logger.logger(2)),
                               data=data,
                               in_sh=in_sh,
                               close_stdin=close_stdin,
                               log_stderr=log_stderr,
                               start_level=start_level,
                               success_level=success_level,
                               error_level=error_level)

    def __call__(self, argv, *args, **opts):
        out, err = None, None
        if "stdout" not in opts:
            opts["stdout"] = subprocess.PIPE if self.data else None
        if "stderr" not in opts:
            opts["stderr"] = subprocess.PIPE if self.log_stderr else None
        try:
            self.log.log(self.start_level, present(argv))
            argv_ = in_sh(argv, not self.data) if self.in_sh else argv
            with open(os.devnull) as devnull:
                if self.close_stdin and "stdin" not in opts:
                    opts["stdin"] = devnull
                p = subprocess.Popen(argv_, *args, **opts)
                out, err = p.communicate()
                code = p.wait()
            if code == 0:
                self.log.log(self.success_level, present(argv, 0))
                if out is not None:
                    self.log.log(self.success_level, "STDOUT // " + out)
                return out
        except subprocess.CalledProcessError as e:
            code = e.returncode
        self.log.log(self.error_level, present(argv, code))
        if err is not None:
            self.log.log(self.error_level, "STDERR // " + err)
        raise subprocess.CalledProcessError(code, argv)


def present(argv, token=None):
    if isinstance(token, basestring):
        return "%s // %s" % (token, escape(argv))
    if isinstance(token, int):
        return "exit %d // %s" % (token, escape(argv))
    return "call // %s" % escape(argv)


def escape(argv):
    # NB: The pipes.quote() function is deprecated in Python 3
    return " ".join(pipes.quote(_) for _ in argv)


def in_sh(argv, allstderr=True):
    """
    Provides better error messages in case of file not found or permission
    denied. Note that this has nothing at all to do with shell=True, since
    quoting prevents the shell from interpreting any arguments -- they are
    passed straight on to shell exec.
    """
    # NB: The use of single and double quotes in constructing the call really
    #     matters.
    call = 'exec "$@" >&2' if allstderr else 'exec "$@"'
    return ["/bin/sh", "-c", call, "sh"] + argv
