from contextlib import contextmanager
import errno
import fcntl
import os
import signal
import subprocess
import time

import deimos.err
from deimos.logger import log
from deimos._struct import _Struct


locks = {}


class LK(_Struct):
    default_timeout = 10

    def __new__(cls, path, flags, seconds=default_timeout):
        if os.path.abspath(path) in locks:
            return locks[path]
        else:
            return super(LK, cls).__new__(cls, path, flags, seconds)

    def __init__(self, path, flags, seconds=default_timeout):
        """Construct a lockable file handle. Handles are recycled.

        If seconds is 0, LOCK_NB will be set. If LOCK_NB is set, seconds will
        be set to 0. If seconds is None, there will be no timeout; but flags
        will not be adjusted in any way.
        """
        full = os.path.abspath(path)
        flags, seconds = nb_seconds(flags, seconds)
        if full not in locks:
            _Struct.__init__(self, path=full,
                                   handle=None,
                                   fd=None,
                                   flags=flags,
                                   seconds=seconds)
            locks[self.path] = self

    def lock(self):
        if self.handle is None or self.handle.closed:
            self.handle = open(self.path, "w+")
            self.fd = self.handle.fileno()
        if (self.flags & fcntl.LOCK_NB) != 0 or self.seconds is None:
            try:
                fcntl.flock(self.handle, self.flags)
            except IOError as e:
                if e.errno not in [errno.EACCES, errno.EAGAIN]:
                    raise e
                raise Locked(self.path)
        else:
            with timeout(self.seconds):
                try:
                    fcntl.flock(self.handle, self.flags)
                except IOError as e:
                    errnos = [errno.EINTR, errno.EACCES, errno.EAGAIN]
                    if e.errno not in errnos:
                        raise e
                    raise Timeout(self.path)

    def unlock(self):
        if not self.handle.closed:
            fcntl.flock(self.handle, fcntl.LOCK_UN)
            self.handle.close()


def format_lock_flags(flags):
    tokens = [("EX", fcntl.LOCK_EX), ("SH", fcntl.LOCK_SH),
               ("UN", fcntl.LOCK_UN), ("NB", fcntl.LOCK_NB)]
    return "|".join(s for s, flag in tokens if (flags & flag) != 0)


def nb_seconds(flags, seconds):
    if seconds == 0:
        flags |= fcntl.LOCK_NB
    if (flags & fcntl.LOCK_NB) != 0:
        seconds = 0
    return flags, seconds


class Err(deimos.err.Err):
    pass


class Timeout(Err):
    pass


class Locked(Err):
    pass


def lock_browser(directory):
    bash = """
        set -o errexit -o nounset -o pipefail

        function files_by_inode {
          find "$1" -type f -printf '%i %p\\n' | LC_ALL=C LANG=C sort
        }

        function locking_pids_by_inode {
          cat /proc/locks |
          sed -r '
            s/^.+ ([^ ]+) +([0-9]+) [^ :]+:[^ :]+:([0-9]+) .+$/\\3 \\2 \\1/
          ' | LC_ALL=C LANG=C sort
        }

        join <(locking_pids_by_inode) <(files_by_inode "$1")
    """
    subprocess.check_call(["bash", "-c", bash, "bash",
                           os.path.abspath(directory)])

# Thanks to Glenn Maynard
# http://stackoverflow.com/questions/5255220/fcntl-flock-how-to-implement-a-timeout/5255473#5255473


@contextmanager
def timeout(seconds):
    def timeout_handler(signum, frame):
        pass
    original_handler = signal.signal(signal.SIGALRM, timeout_handler)
    try:
        signal.alarm(seconds)
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)
