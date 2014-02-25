import glob
import itertools
import json
import logging
import re
import subprocess
import sys
import time

from medea.err import *


def run(options, image, command=[], env={}, cpus=None, mems=None, ports=[]):
    envs  = env.items() if isinstance(env, dict) else env
    pairs = [ ("-e", "%s=%s" % (k, v)) for k, v in envs ]
    if ports != []:               # NB: Forces external call to pre-fetch image
        port_pairings = list(itertools.izip_longest(ports, inner_ports(image)))
        log.info("Port pairings (Mesos, Docker) // %r", port_pairings)
        for allocated, target in port_pairings:
            if allocated is None or target is None: break
            options += [ "-p", "%d:%d" % (allocated, target) ]
    argv  = [ "docker", "run" ] + options
    argv += [ "-c", str(cpus) ] if cpus else []
    argv += [ "-m", str(mems) ] if mems else []
    argv += [ _ for __ in pairs for _ in __ ]            # This is just flatten
    argv += [ image ] + command
    return argv

def stop(ident):
    return ["docker", "stop", "-t=2", ident]

def rm(ident):
    return ["docker", "rm", ident]

def wait(ident):
    return ["docker", "wait", ident]


images = {} ######################################## Cache of image information

def pull(image):
    subprocess.check_call(["docker", "pull", image])
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
    delim   = re.compile("  +")
    text    = subprocess.check_output(["docker", "images", image])
    records = [ delim.split(line) for line in text.splitlines() ]
    for record in records:
        if record[0] == image:
            text   = subprocess.check_output(["docker", "inspect", image])
            parsed = json.loads(text)[0]
            images[image] = parsed
            return parsed

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
            return [ int(k.split("/")[0]) for k in exposed.keys() ]
        specs = config.get("PortSpecs", [])
        if specs and isinstance(specs, list):
            return [ int(v.split(":")[-1]) for v in specs ]
    return [] # If all else fails...


################################################# System and process interfaces

def root_pid(ident):
    """Lookup the root PID for the given container.
    This is the PID that corresponds the `lxc-start` command at the root of
    the container's process tree.
    """
    fetch_lxc_pid = """ ps -C lxc-start -o pid= -o args= | # Look for lxc-start
                        fgrep -- " -n $1" |           # Just for this container
                        cut -d" " -f1                       # Keep only the PID
                    """
    argv = ["sh", "-c", fetch_lxc_pid.strip(), "sh", canonical_id(ident)]
    return subprocess.check_output(argv).strip()

def cgroups(ident):
    paths = glob.glob("/sys/fs/cgroup/*/" + canonical_id(ident))
    return dict( (s.split("/")[-2], s) for s in paths )

def canonical_id(ident):
    argv = ["docker", "inspect", "--format={{.ID}}", ident]
    return subprocess.check_output(argv).strip()

def exists(ident):
    try:
        argv = ["docker", "inspect", "--format={{.ID}}", ident]
        with open("/dev/null", "w") as dev_null:
            subprocess.check_call(argv, stdout=dev_null, stderr=dev_null)
    except subprocess.CalledProcessError as e:
        if e.returncode != 1: raise e
        return False
    return True

def await(ident, t=0.05, n=10):
    for _ in range(0, n):
        if exists(ident): return
        time.sleep(t)
    if exists(ident): return
    log.warning("Container not ready after %d sleeps of %g seconds", n, t)
    raise AwaitTimeout()

log = logging.getLogger(__name__)

class AwaitTimeout(Err):
    pass
