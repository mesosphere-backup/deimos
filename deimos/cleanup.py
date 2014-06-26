from fcntl import LOCK_EX, LOCK_NB
import glob
import os
import subprocess
import time

from deimos.cmd import Run
import deimos.flock
from deimos.logger import log
from deimos.timestamp import iso
from deimos._struct import _Struct


class Cleanup(_Struct):

    def __init__(self, root="/tmp/deimos", optimistic=False):
        _Struct.__init__(self, root=root,
                               optimistic=optimistic,
                               lock=os.path.join(root, "cleanup"))

    def dirs(self, before=time.time(), exited=True):
        """
        Provider a generator of container state directories.

        If exited is None, all are returned. If it is False, unexited
        containers are returned. If it is True, only exited containers are
        returned.
        """
        timestamp = iso(before)
        root = os.path.join(self.root, "start-time")
        os.chdir(root)
        by_t = (d for d in glob.iglob("????-??-??T*.*Z") if d < timestamp)
        if exited is None:
            def predicate(directory):
                return True
        else:
            def predicate(directory):
                exit = os.path.join(directory, "exit")
                return os.path.exists(exit) is exited
        return (os.path.join(root, d) for d in by_t if predicate(d))

    def remove(self, *args, **kwargs):
        errors = 0
        lk = deimos.flock.LK(self.lock, LOCK_EX | LOCK_NB)
        try:
            lk.lock()
        except deimos.flock.Err:
            msg = "Lock unavailable -- is cleanup already running?"
            if self.optimistic:
                log.info(msg)
                return 0
            else:
                log.error(msg)
                raise e
        try:
            for d in self.dirs(*args, **kwargs):
                state = deimos.state.state(d)
                if state is None:
                    log.warning("Not able to load state from: %s", d)
                    continue
                try:
                    cmd = ["rm", "-rf", d + "/"]
                    cmd += [state._mesos()]
                    if state.cid() is not None:
                        cmd += [state._docker()]
                    Run()(cmd)
                except subprocess.CalledProcessError:
                    errors += 1
        finally:
            lk.unlock()
        if errors != 0:
            log.error("There were failures on %d directories", errors)
            return 4
