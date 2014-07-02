import inspect
import logging
import os
import re

try:                  # Prefer system installation of Mesos protos if available
    from mesos_pb2 import *
    from containerizer_pb2 import *
except:
    from deimos.mesos_pb2 import *
    from deimos.containerizer_pb2 import *

import deimos.cmd
from deimos.logger import log
from deimos.proto import recordio


class Containerizer(object):

    def __init__(self):
        pass

    def launch(self, launch_pb, *args):
        pass

    def update(self, update_pb, *args):
        pass

    def usage(self, usage_pb, *args):
        pass

    def wait(self, wait_pb, *args):
        pass

    def destroy(self, destroy_pb, *args):
        pass

    def recover(self, *args):
        pass

    def containers(self, *args):
        pass

    def observe(self, *args):
        pass


def methods():
    "Names of operations provided by containerizers, as a set."
    pairs = inspect.getmembers(Containerizer, predicate=inspect.ismethod)
    return set(k for k, _ in pairs if k[0:1] != "_")

# Not an instance method of containerizer because it shouldn't be overridden.


def stdio(containerizer, *args):
    """Connect containerizer class to command line args and STDIN

    Dispatches to an appropriate containerizer method based on the first
    argument and parses the input using an appropriate Protobuf type.

        launch < containerizer::Launch
        update < containerizer::Update
        usage < containerizer::Usage > mesos::ResourceStatistics
        wait < containerizer::Wait > containerizer::Termination
        destroy < containerizer::Destroy
        containers > containerizer::Containers
        recover

    Output serialization must be handled by the containerizer method (it
    doesn't necessarily happen at the end).

    Not really part of the containerizer protocol but exposed by Deimos as a
    subcommand:

        # Follows a Docker ID, PID, &c and exits with an appropriate, matching
        # exit code, in a manner specific to the containerizer
        observe <id>

    """
    try:
        name = args[0]
        method, proto = {"launch": (containerizer.launch, Launch),
                          "update": (containerizer.update, Update),
                          "usage": (containerizer.usage, Usage),
                          "wait": (containerizer.wait, Wait),
                          "destroy": (containerizer.destroy, Destroy),
                          "containers": (containerizer.containers, None),
                          "recover": (containerizer.recover, None),
                          "observe": (containerizer.observe, None)}[name]
    except IndexError:
        raise Err("Please choose a subcommand")
    except KeyError:
        raise Err("Subcommand %s is not valid for containerizers" % name)
    log.debug("%r", (method, proto))
    if proto is not None:
        return method(recordio.read(proto), *args[1:])
    else:
        return method(*args[1:])


# Mesos interface helpers

MESOS_ESSENTIAL_ENV = ["MESOS_SLAVE_ID", "MESOS_SLAVE_PID",
                        "MESOS_FRAMEWORK_ID", "MESOS_EXECUTOR_ID",
                        "MESOS_CHECKPOINT", "MESOS_RECOVERY_TIMEOUT"]


def mesos_env():
    env = os.environ.get
    return [(k, env(k)) for k in MESOS_ESSENTIAL_ENV if env(k)]


def log_mesos_env(level=logging.INFO):
    for k, v in os.environ.items():
        if k.startswith("MESOS_") or k.startswith("LIBPROCESS_"):
            log.log(level, "%s=%s" % (k, v))


def mesos_directory():
    if "MESOS_DIRECTORY" not in os.environ:
        return
    work_dir = os.path.abspath(os.getcwd())
    task_dir = os.path.abspath(os.environ["MESOS_DIRECTORY"])
    if task_dir != work_dir:
        log.info("Changing directory to MESOS_DIRECTORY=%s", task_dir)
        os.chdir(task_dir)


def mesos_executor():
    return os.path.join(os.environ["MESOS_LIBEXEC_DIRECTORY"],
                        "mesos-executor")


def mesos_default_image():
    return os.environ.get("MESOS_DEFAULT_CONTAINER_IMAGE")


def place_uris(launchy, directory, optimistic_unpack=False):
    cmd = deimos.cmd.Run()
    cmd(["mkdir", "-p", directory])
    for item in launchy.uris:
        uri = item.value
        gen_unpack_cmd = unpacker(uri) if optimistic_unpack else None
        log.info("Retrieving URI: %s", deimos.cmd.escape([uri]))
        try:
            basename = uri.split("/")[-1]
            f = os.path.join(directory, basename)
            if basename == "":
                raise IndexError
        except IndexError:
            log.info("Not able to determine basename: %r", uri)
            continue
        try:
            cmd(fetcher_command(uri, f))
        except subprocess.CalledProcessError as e:
            log.warning("Failed while processing URI: %s",
                        deimos.cmd.escape([uri]))
            continue
        if item.executable:
            os.chmod(f, 0755)
        if gen_unpack_cmd is not None:
            log.info("Unpacking %s" % f)
            cmd(gen_unpack_cmd(f, directory))
            cmd(["rm", "-f", f])


def fetcher_command(uri, target):
    if uri[0:5] == "s3://":
        return ["aws", "s3", "cp", uri, target]
    return ["curl", "-sSfL", uri, "--output", target]


def unpacker(uri):
    if re.search(r"[.](t|tar[.])(bz2|xz|gz)$", uri):
        return lambda f, directory: ["tar", "-C", directory, "-xf", f]
    if re.search(r"[.]zip$", uri):
        return lambda f, directory: ["unzip", "-d", directory, f]
