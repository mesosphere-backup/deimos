import glob
import itertools
import json
import logging
import os
import re
import subprocess
import sys
import time

from deimos.cmd import Run
from deimos.err import *
from deimos.logger import log
from deimos._struct import _Struct


def run(options, image, command=[], env={}, cpus=None, mems=None, ports=[]):
    envs = env.items() if isinstance(env, dict) else env
    pairs = [("-e", "%s=%s" % (k, v)) for k, v in envs]
    if ports != []:               # NB: Forces external call to pre-fetch image
        port_pairings = list(itertools.izip_longest(ports, inner_ports(image)))
        log.info("Port pairings (Mesos, Docker) // %r", port_pairings)
        for allocated, target in port_pairings:
            if allocated is None:
                log.warning("Container exposes more ports than were allocated")
                break
            options += ["-p", "%d:%d" % (allocated, target or allocated)]
    argv = ["run"] + options
    argv += ["-c", str(cpus)] if cpus else []
    argv += ["-m", str(mems)] if mems else []
    argv += [_ for __ in pairs for _ in __]              # This is just flatten
    argv += [image] + command
    return docker(*argv)


def stop(ident):
    return docker("stop", "-t=2", ident)


def rm(ident):
    return docker("rm", ident)


def wait(ident):
    return docker("wait", ident)


images = {}                                        # Cache of image information


def pull(image):
    Run(data=True)(docker("pull", image))
    refresh_docker_image_info(image)


def pull_once(image):
    if image not in images:
        pull(image)


def image_info(image):
    if image in images:
        return images[image]
    else:
        return refresh_docker_image_info(image)


def refresh_docker_image_info(image):
    try:
        text = Run(data=True)(docker("inspect", image))
        parsed = json.loads(text)[0]
        images[image] = parsed
        return parsed
    except subprocess.CalledProcessError as e:
        return None


def ensure_image(f):
    def f_(image, *args, **kwargs):
        pull_once(image)
        return f(image, *args, **kwargs)
    return f_


@ensure_image
def inner_ports(image):
    info = image_info(image)
    config = info.get("Config", info.get("config"))
    if config:
        exposed = config.get("ExposedPorts", {})
        if exposed and isinstance(exposed, dict):
            return sorted(int(k.split("/")[0]) for k in exposed.keys())
        specs = config.get("PortSpecs", [])
        if specs and isinstance(specs, list):
            return sorted(int(v.split(":")[-1]) for v in specs)
    return []  # If all else fails...


# System and process interfaces

class Status(_Struct):

    def __init__(self, cid=None, pid=None, exit=None):
        _Struct.__init__(self, cid=cid, pid=pid, exit=exit)


def cgroups(cid):
    paths = []
    paths += glob.glob("/sys/fs/cgroup/*/" + cid)
    paths += glob.glob("/sys/fs/cgroup/*/docker/" + cid)
    return dict((s.split("/")[4], s) for s in paths)


def matching_image_for_host(distro=None, release=None, *args, **kwargs):
    if distro is None or release is None:
        # TODO: Use redhat-release, &c
        rel_string = Run(data=True)(["bash", "-c", """
            set -o errexit -o nounset -o pipefail
            ( source /etc/os-release && tr A-Z a-z <<<"$ID\t$VERSION_ID" )
        """])
        probed_distro, probed_release = rel_string.strip().split()
        distro, release = (distro or probed_distro, release or probed_release)
    return image_token("%s:%s" % (distro, release), *args, **kwargs)


def image_token(name, account=None, index=None):
    return "/".join(_ for _ in [index, account, name] if _ is not None)


def probe(ident, quiet=False):
    fields = "{{.ID}} {{.State.Pid}} {{.State.ExitCode}}"
    level = logging.DEBUG if quiet else logging.WARNING
    argv = docker("inspect", "--format=" + fields, ident)
    run = Run(data=True, error_level=level)
    text = run(argv).strip()
    cid, pid, exit = text.split()
    return Status(cid=cid, pid=pid, exit=(exit if pid == 0 else None))


def exists(ident, quiet=False):
    try:
        return probe(ident, quiet)
    except subprocess.CalledProcessError as e:
        if e.returncode != 1:
            raise e
        return None


def await(ident, t=0.05, n=10):
    for _ in range(0, n):
        result = exists(ident, quiet=True)
        if result:
            return result
        time.sleep(t)
    result = exists(ident, quiet=True)
    if result:
        return result
    msg = "Container %s not ready after %d sleeps of %g seconds"
    log.warning(msg % (ident, n, t))
    raise AwaitTimeout("Timed out waiting for %s" % ident)


def read_wait_code(data):
    try:
        code = int(data)
        code = 128 + abs(code) if code < 0 else code
        return code % 256
    except:
        log.error("Result of `docker wait` wasn't an int: %r", data)
        return 111


class AwaitTimeout(Err):
    pass


# Global settings

options = []


def docker(*args):
    return ["docker"] + options + list(args)
