import base64
import errno
from fcntl import LOCK_EX, LOCK_NB, LOCK_SH, LOCK_UN
import inspect
import logging
import os
import random
import re
import signal
import subprocess32 as subprocess
import sys
import time

try:                  # Prefer system installation of Mesos protos if available
    from mesos_pb2 import *
    from containerizer_pb2 import *
except:
    from deimos.mesos_pb2 import *
    from deimos.containerizer_pb2 import *

import deimos.cgroups
from deimos.cmd import Run
import deimos.config
import deimos.containerizer
import deimos.docker
from deimos.err import Err
import deimos.logger
from deimos.logger import log
import deimos.mesos
import deimos.path
from deimos.proto import recordio
from deimos._struct import _Struct
import deimos.state
import deimos.sig

import itertools
import functools
import json
import requests
import urlparse
import shlex
import glob

STATE_REFRESH = 1

class Handler(deimos.containerizer.Containerizer, _Struct):
    _gear_host = "http://localhost:43273/"

    def __init__(self, workdir="/tmp/mesos-sandbox",
                       state_root="/tmp/deimos",
                       shared_dir="fs",
                       optimistic_unpack=True,
                       container_settings=deimos.config.Containers(),
                       index_settings=deimos.config.DockerIndex()):
        _Struct.__init__(self, workdir=workdir,
                               state_root=state_root,
                               shared_dir=shared_dir,
                               optimistic_unpack=optimistic_unpack,
                               container_settings=container_settings,
                               index_settings=index_settings,
                               runner=None,
                               state=None)

    def _docker_cid(self, container_id):
        try:
            call = ["docker", "inspect", "-f", "{{.ID}}", container_id]
            return deimos.cmd.Run(data=True)(call).strip()
        except subprocess.CalledProcessError:
            return ""

    def _id(self, cls):
        msg = recordio.read(cls)
        return msg.container_id.value[:23]

    def _container(self, id, image, opts, ports):
        call = [ "gear", "install", "--start=true" ]
        call += opts

        if len(ports) != 0:
            cmd_ports = ["%s:%s" % (i, e) for e, i in ports]
            call += [ "-p", ','.join(cmd_ports) ]

        call += [ image, id ]
        deimos.cmd.Run(data=True)(call)

    def _observer(self, id):
        observer_argv = [ deimos.containerizer.mesos_executor(), "--override",
            deimos.path.me(), "wait", "@@observe-geard@@", id ]

        log.error(observer_argv)
        log.info(deimos.cmd.present(observer_argv))
        call = deimos.cmd.in_sh(observer_argv, allstderr=False)

        # If the Mesos executor sees LIBPROCESS_PORT=0 (which
        # is passed by the slave) there are problems when it
        # attempts to bind. ("Address already in use").
        # Purging both LIBPROCESS_* net variables, to be safe.
        for v in ["LIBPROCESS_PORT", "LIBPROCESS_IP"]:
            if v in os.environ:
                del os.environ[v]
        subprocess.Popen(call, close_fds=True)

    # TODO: check for geard running first
    def launch(self, *args):
        log.info(" ".join(args))

        proto = recordio.read(Launch)
        launchy = deimos.mesos.Launch(proto)

        deimos.containerizer.place_uris(launchy, self.shared_dir,
            self.optimistic_unpack)

        container_id = launchy.container_id[:23]

        image = self._image(launchy)
        ports = list(itertools.izip_longest(launchy.ports,
            deimos.docker.inner_ports(image)))
        opts = launchy.container[1] if len(launchy.container) > 0 else []

        self._container(container_id, image, opts, ports)

        self._observer(container_id)

        return 0

    def update(self, *args):
        log.info(" ".join(args))
        log.info("Update is a no-op for Docker...")


    def cgroups(self, cid):
        paths = glob.glob("/sys/fs/cgroup/*/*/docker-%s.scope" % (cid,))
        return dict( (s.split("/")[4], s) for s in paths )

    def usage(self, *args):
        log.info(" ".join(args))

        id = self._id(Usage)

        docker_cid = self._docker_cid(id)
        if id == "":
            log.info("Container not running?")
            return 0

        cg = deimos.cgroups.CGroups(**self.cgroups(docker_cid))
        if len(cg.keys()) == 0:
            log.info("Container has no CGroups...already stopped?")
            return 0
        try:
            recordio.write(ResourceStatistics,
                           timestamp             = time.time(),
                           mem_limit_bytes       = cg.memory.limit(),
                           cpus_limit            = cg.cpu.limit(),
                         # cpus_user_time_secs   = cg.cpuacct.user_time(),
                         # cpus_system_time_secs = cg.cpuacct.system_time(),
                           mem_rss_bytes         = cg.memory.rss())
        except AttributeError as e:
            log.error("Missing CGroup!")
            raise e
        return 0

    def _status(self, id):
        resp = requests.get(urlparse.urljoin(self._gear_host, "/containers"))

        # Minor hiccup in geard, retry again
        if resp.status_code == 503:
            return True

        state = [x for x in resp.json()["Containers"] if x["Id"] == id]

        if len(state) == 0: return False
        state = state[0]

        if state["ActiveState"] not in ["active", "activating"]:
            return False

        return True

    def wait(self, *args):
        log.info(" ".join(args))

        if list(args[0:1]) in [ ["@@observe-geard@@"] ]:
            id = args[1]
        else:
            id = self._id(Wait)

        def kill(*args):
            self.halt(id)
            return deimos.sig.Resume()

        deimos.sig.install(kill)

        while self._status(id):
            time.sleep(STATE_REFRESH)

        recordio.write(Termination,
                       killed  = False,
                       message = "",
                       status  = 0 << 8)
        return 0

    def destroy(self, *args):
        log.info(" ".join(args))

        self.halt(self._id(Destroy))

        return 0

    def halt(self, id, *args):
        resp = requests.delete(urlparse.urljoin(
            self._gear_host, "/container/%s" % (id,)))

    def containers(self, *args):
        log.info(" ".join(args))

        recordio.writeProto(Containers())
        return 0
