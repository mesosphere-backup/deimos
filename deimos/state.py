from contextlib import contextmanager
import errno
import fcntl
import os
import signal

from deimos.err import *
from deimos.logger import log
from deimos._struct import _Struct


class State(_Struct):
    def __init__(self, root, docker_id=None, mesos_id=None, task_id=None):
        _Struct.__init__(self, root=os.path.abspath(root),
                               docker_id=docker_id,
                               mesos_id=mesos_id,
                               task_id=task_id)
    def resolve(self, *args, **kwargs):
        if self.mesos_id is not None:
            return self._mesos(*args, **kwargs)
        else:
            return self._docker(*args, **kwargs)
    def mesos_container_id(self):
        if self.mesos_id is None:
            self.mesos_id = self._readf("mesos-container-id")
        return self.mesos_id
    def tid(self):
        if self.task_id is None:
            self.task_id = self._readf("tid")
        return self.task_id
    def sandbox_symlink(self, value=None):
        p = self.resolve("fs")
        if value is not None:
            link(value, p)
        return p
    def pid(self, value=None):
        if value is not None:
            self._writef("pid", str(value))
        return self._readf("pid")
    def cid(self):
        if self.docker_id is None:
            self.docker_id = self._readf("cid")
        return self.docker_id
    def lock(self, name, flags, seconds=60):
        if (flags & fcntl.LOCK_NB) != 0:
            log.error("This function must be called with blocking flags")
            raise Err("Bad lock spec")
        p = self.resolve(os.path.join("lock", name), mkdir=True)
        handle = flock(p, flags, seconds)
        if handle is None:
            log.error("Waited %d seconds for %s", seconds, name)
            raise FLockTimeout()
        return handle
    def exit(self, value=None):
        if value is not None:
            self._writef("exit", str(value))
        return self._readf("exit")
    def push(self):
        self._mkdir()
        properties = [("cid", self.docker_id),
                      ("mesos-container-id", self.mesos_id),
                      ("tid", self.task_id)]
        for k, v in properties:
            if v is not None and not os.path.exists(self.resolve(k)):
                self._writef(k, v)
        if self.docker_id is not None:
            docker = os.path.join(self.root, "docker", self.docker_id)
            link("../mesos/" + self.mesos_id, docker)
    def _mkdir(self):
        create(os.path.join(self.root, "mesos", self.mesos_id))
    def _readf(self, path):
        f = self.resolve(path)
        if os.path.exists(f):
            with open(f) as h:
                return h.read().strip()
    def _writef(self, path, value):
        f = self.resolve(path)
        with open(f, "w+") as h:
            h.write(value + "\n")
    def _docker(self, path, mkdir=False):
        p = os.path.join(self.root, "docker", self.docker_id, path)
        p = os.path.abspath(p)
        if mkdir:
            create(os.path.dirname(p))
        return p
    def _mesos(self, path, mkdir=False):
        p = os.path.join(self.root, "mesos", self.mesos_id, path)
        p = os.path.abspath(p)
        if mkdir:
            create(os.path.dirname(p))
        return p
    def ids(self):
        if self.tid() is not None:
            log.info("task   = %s", self.tid())
        if self.mesos_container_id() is not None:
            log.info("mesos  = %s", self.mesos_container_id())
        if self.cid() is not None:
            log.info("docker = %s", self.cid())

def create(path):
    if not os.path.exists(path):
        os.makedirs(path)

def link(source, target):
    if not os.path.exists(target):
        create(os.path.dirname(target))
        os.symlink(source, target)


class FLockTimeout(Err): pass

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

def flock(path, flags, seconds=10):
    with timeout(seconds):
        h = open(path, "w+")
        try:
            fcntl.flock(h, flags)
        except IOError as e:
            if e.errno not in [errno.EINTR, errno.EACCESS, errno.EAGAIN]:
                raise e
            return None
        return h

