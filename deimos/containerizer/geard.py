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

import functools
import json
import requests
import urlparse
import shlex

STATE_REFRESH = 1

class Geard(deimos.containerizer.Containerizer, _Struct):
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

    def _cid(self, container_id):
        try:
            return subprocess.check_output(
                shlex.split('docker inspect -f "{{.ID}}" %s' % (
                    container_id, )))[:-1]
        except subprocess.CalledProcessError:
            return ""

    def watch_observer(self, observer):
        thread = threading.Thread(target=observer.wait)
        thread.start()
        thread.join(10)

    # TODO: check for geard running first
    def launch(self, *args):
        log.info(" ".join(args))

        proto = recordio.read(Launch)
        launchy = deimos.mesos.Launch(proto)
        log.error(launchy)

        if launchy.directory:
            os.chdir(launchy.directory)

        deimos.containerizer.place_uris(launchy, self.shared_dir,
            self.optimistic_unpack)


        container_id = launchy.container_id[:23]
        resp = requests.put(urlparse.urljoin(
            self._gear_host, "/container/%s" % (container_id,)),
            headers={
                "Content-Type": "application/json"
            }, data=json.dumps({
                "Image": self._image(launchy),
                "Started": True
            }))
        log.error(resp.content)

        observer_argv = [ deimos.containerizer.mesos_executor(), "--override",
                              deimos.path.me(), "wait", "@@observe-docker@@" ]

        observer_argv += [container_id]
        log.info(deimos.cmd.present(observer_argv))
        call = deimos.cmd.in_sh(observer_argv, allstderr=False)


        log.error(call)
        # If the Mesos executor sees LIBPROCESS_PORT=0 (which
        # is passed by the slave) there are problems when it
        # attempts to bind. ("Address already in use").
        # Purging both LIBPROCESS_* net variables, to be safe.
        for v in ["LIBPROCESS_PORT", "LIBPROCESS_IP"]:
            if v in os.environ:
                del os.environ[v]
        subprocess.Popen(call, close_fds=True)
        return 0

    def update(self, *args):
        log.info(" ".join(args))
        log.info("Update is a no-op for Docker...")

    def usage(self, *args):
        log.info(" ".join(args))
        message = recordio.read(Usage)
        container_id = message.container_id.value[:23]

        docker_cid = self._cid(container_id)
        if docker_cid == "":
            log.info("Container not running?")
            return 0

        cg = deimos.cgroups.CGroups(**deimos.docker.cgroups(docker_cid))
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

    def wait(self, *args):
        log.info(" ".join(args))

        if list(args[0:1]) in [ ["--observe-docker"], ["@@observe-docker@@"] ]:
            container_id = args[1]
        else:
            message = recordio.read(Wait)
            container_id = message.container_id.value[:23]

        def kill(*args):
            self.halt(container_id)
            return deimos.sig.Resume()

        deimos.sig.install(kill)

        status = 0
        while True:
            resp = requests.get(urlparse.urljoin(
                self._gear_host, "/containers"))

            state = [x for x in resp.json()["Containers"]
                if x["Id"] == container_id]

            if len(state) == 0:
                break
            state = state[0]

            if state["ActiveState"] not in ["active", "activating"]:
                status = 1
                break

            time.sleep(STATE_REFRESH)

        recordio.write(Termination,
                       killed  = False,
                       message = "",
                       status  = 64 << 8)
        return 0

    def destroy(self, *args):
        log.info(" ".join(args))
        message = recordio.read(Destroy)
        container_id = message.container_id.value[:23]

        self.halt(container_id)

        return 0

    def halt(self, id, *args):
        resp = requests.delete(urlparse.urljoin(
            self._gear_host, "/container/%s" % (id,)))

    def default_image(self, launchy):
        opts = dict(self.index_settings.items(onlyset=True))
        if "account_libmesos" in opts:
            if not launchy.needs_observer:
                opts["account"] = opts["account_libmesos"]
            del opts["account_libmesos"]
        return deimos.docker.matching_image_for_host(**opts)

    def containers(self, *args):
        log.info(" ".join(args))

        recordio.writeProto(Containers())
        return 0
