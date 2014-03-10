import logging
import os
import pipes
import subprocess
import sys

import deimos.logger
from deimos.err import *
from deimos._struct import _Struct


class Run(_Struct):
    def __init__(self, log=None, data=False, in_sh=True, close_stdin=True,
                       start_level=logging.DEBUG,
                       success_level=logging.DEBUG,
                       error_level=logging.WARNING):
        _Struct.__init__(self, log   = log if log else deimos.logger.logger(2),
                               data  = data,
                               in_sh = in_sh,
                               close_stdin   = close_stdin,
                               start_level   = start_level,
                               success_level = success_level,
                               error_level   = error_level)
    def __call__(self, argv, *args, **opts):
        runner = subprocess.check_output if self.data else subprocess.check_call
        try:
            self.log.log(self.start_level, present(argv))
            argv_  = in_sh(argv, not self.data) if self.in_sh else argv
            if self.close_stdin and "stdin" not in opts:
                with open(os.devnull) as devnull:
                    opts["stdin"] = devnull
                    result = runner(argv_, *args, **opts)
            else:
                result = runner(argv_, *args, **opts)
            self.log.log(self.success_level, present(argv, 0))
            return result
        except subprocess.CalledProcessError as e:
            self.log.log(self.error_level, present(argv, e.returncode))
            raise e

def present(argv, exit=None):
    if exit is not None:
        return "exit %d // %s" % (exit, escape(argv))
    else:
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

# This try block is here to upgrade functionality available the subprocess
# module for older versions of Python. As last as 2.6, subprocess did not have
# the check_output function.
try:
    subprocess.check_output
except:
    def check_output(*args):
        p = subprocess.Popen(stdout=subprocess.PIPE, *args)
        stdout = p.communicate()[0]
        exitcode = p.wait()
        if exitcode:
            raise subprocess.CalledProcessError(exitcode, args[0])
        return stdout
    subprocess.check_output = check_output

