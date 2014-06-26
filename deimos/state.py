import errno
from fcntl import LOCK_EX, LOCK_NB, LOCK_SH, LOCK_UN
import itertools
import os
import random
import signal
import time

import deimos.docker
from deimos.err import *
from deimos.logger import log
from deimos._struct import _Struct
from deimos.timestamp import iso


class State(_Struct):

    def __init__(self, root, docker_id=None, mesos_id=None, executor_id=None):
        _Struct.__init__(self, root=os.path.abspath(root),
                               docker_id=docker_id,
                               mesos_id=mesos_id,
                               executor_id=executor_id,
                               timestamp=None)

    def resolve(self, *args, **kwargs):
        if self.mesos_id is not None:
            return self._mesos(*args, **kwargs)
        else:
            return self._docker(*args, **kwargs)

    def mesos_container_id(self):
        if self.mesos_id is None:
            self.mesos_id = self._readf("mesos-container-id")
        return self.mesos_id

    def eid(self):
        if self.executor_id is None:
            self.executor_id = self._readf("eid")
        return self.executor_id

    def sandbox_symlink(self, value=None):
        p = self.resolve("fs")
        if value is not None:
            link(value, p)
        return p

    def pid(self, value=None):
        if value is not None:
            self._writef("pid", str(value))
        data = self._readf("pid")
        if data is not None:
            return int(data)

    def cid(self, refresh=False):
        if self.docker_id is None or refresh:
            self.docker_id = self._readf("cid")
        return self.docker_id

    def t(self):
        if self.timestamp is None:
            self.timestamp = self._readf("t")
        return self.timestamp

    def await_cid(self, seconds=60):
        base = 0.05
        start = time.time()
        steps = [1.0, 1.25, 1.6, 2.0, 2.5, 3.2, 4.0, 5.0, 6.4, 8.0]
        scales = (10.0 ** n for n in itertools.count())
        scaled = ([scale * step for step in steps] for scale in scales)
        sleeps = itertools.chain.from_iterable(scaled)
        log.info("Awaiting CID file: %s", self.resolve("cid"))
        while self.cid(refresh=True) in [None, ""]:
            time.sleep(next(sleeps))
            if time.time() - start >= seconds:
                raise CIDTimeout("No CID file after %ds" % seconds)

    def await_launch(self):
        lk_l = self.lock("launch", LOCK_SH)
        self.ids(3)
        if self.cid() is None:
            lk_l.unlock()
            self.await_cid()
            lk_l = self.lock("launch", LOCK_SH)
        return lk_l

    def lock(self, name, flags, seconds=60):
        fmt_time = "indefinite" if seconds is None else "%ds" % seconds
        fmt_flags = deimos.flock.format_lock_flags(flags)
        flags, seconds = deimos.flock.nb_seconds(flags, seconds)
        log.info("request // %s %s (%s)", name, fmt_flags, fmt_time)
        p = self.resolve(os.path.join("lock", name), mkdir=True)
        lk = deimos.flock.LK(p, flags, seconds)
        try:
            lk.lock()
        except deimos.flock.Err:
            log.error("failure // %s %s (%s)", name, fmt_flags, fmt_time)
            raise
        if (flags & LOCK_EX) != 0:
            lk.handle.write(iso() + "\n")
        log.info("success // %s %s (%s)", name, fmt_flags, fmt_time)
        return lk

    def exit(self, value=None):
        if value is not None:
            self._writef("exit", str(value))
        data = self._readf("exit")
        if data is not None:
            return deimos.docker.read_wait_code(data)

    def push(self):
        self._mkdir()
        properties = [("cid", self.docker_id),
                      ("mesos-container-id", self.mesos_id),
                      ("eid", self.executor_id)]
        self.set_start_time()
        for k, v in properties:
            if v is not None and not os.path.exists(self.resolve(k)):
                self._writef(k, v)
        if self.cid() is not None:
            docker = os.path.join(self.root, "docker", self.cid())
            link("../mesos/" + self.mesos_id, docker)

    def set_start_time(self):
        if self.t() is not None:
            return
        d = os.path.abspath(os.path.join(self.root, "start-time"))
        create(d)
        start, t = time.time(), iso()
        while time.time() - start <= 1.0:
            try:
                p = os.path.join(d, t)
                os.symlink("../mesos/" + self.mesos_id, p)
                self._writef("t", t)
                self.timestamp = t
                return
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                time.sleep(random.uniform(0.005, 0.025))
                t = iso()

    def _mkdir(self):
        create(self._mesos())

    def _readf(self, path):
        f = self.resolve(path)
        if os.path.exists(f):
            with open(f) as h:
                return h.read().strip()

    def _writef(self, path, value):
        f = self.resolve(path)
        with open(f, "w+") as h:
            h.write(value + "\n")
            h.flush()

    def _docker(self, path=None, mkdir=False):
        if path is None:
            p = os.path.join(self.root, "docker", self.docker_id)
        else:
            p = os.path.join(self.root, "docker", self.docker_id, path)
        p = os.path.abspath(p)
        if mkdir:
            docker = os.path.join(self.root, "docker", self.docker_id)
            if not os.path.exists(docker):
                log.error("No Docker symlink (this should be impossible)")
                raise Err("Bad Docker symlink state")
            create(os.path.dirname(p))
        return p

    def _mesos(self, path=None, mkdir=False):
        if path is None:
            p = os.path.join(self.root, "mesos", self.mesos_id)
        else:
            p = os.path.join(self.root, "mesos", self.mesos_id, path)
        p = os.path.abspath(p)
        if mkdir:
            create(os.path.dirname(p))
        return p

    def ids(self, height=2):
        log = deimos.logger.logger(height)
        if self.eid() is not None:
            log.info("eid    = %s", self.eid())
        if self.mesos_container_id() is not None:
            log.info("mesos  = %s", self.mesos_container_id())
        if self.cid() is not None:
            log.info("docker = %s", self.cid())

    def exists(self):
        path = None
        if self.mesos_id is not None:
            path = os.path.join(self.root, "mesos", self.mesos_id)
        if self.docker_id is not None:
            path = os.path.join(self.root, "docker", self.docker_id)
        if path is not None:
            return os.path.exists(path)
        return False


class CIDTimeout(Err):
    pass


def create(path):
    if not os.path.exists(path):
        os.makedirs(path)


def link(source, target):
    if not os.path.exists(target):
        create(os.path.dirname(target))
        os.symlink(source, target)


def state(directory):
    mesos = os.path.join(directory, "mesos-container-id")
    if os.path.exists(mesos):
        with open(mesos) as h:
            mesos_id = h.read().strip()
        root = os.path.dirname(os.path.dirname(os.path.realpath(directory)))
        return State(root=root, mesos_id=mesos_id)
