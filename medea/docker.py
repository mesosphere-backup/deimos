import itertools
import json
import re
import subprocess
import sys


def run(options, image, command=[], env={}, cpus=None, mems=None, ports=[]):
    envs  = env.items() if isinstance(env, dict) else env
    pairs = [ ("-e", "%s=%s" % (k, v)) for k, v in envs ]
    if ports != []:               # NB: Forces external call to pre-fetch image
        port_pairings = list(itertools.izip_longest(ports, inner_ports(image)))
        print >>sys.stderr, "Port pairings (Mesos,Docker) //", port_pairings
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

