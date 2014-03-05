import base64
import inspect
import logging
import os
import random
import re
import signal
import subprocess
import sys
import time

try:    import mesos_pb2 as protos                 # Prefer system installation
except: import deimos.mesos_pb2 as protos

import deimos.cgroups
from deimos.cmd import Run
import deimos.config
import deimos.containerizer
import deimos.docker
from deimos.err import Err
import deimos.logger
from deimos.logger import log
from deimos._struct import _Struct


class Containerizer(object):
    def __init__(self): pass
    def launch(self, container_id, *args): pass
    def update(self, container_id, *args): pass
    def usage(self, container_id, *args): pass
    def wait(self, container_id, *args): pass
    def destroy(self, container_id, *args): pass
    def __call__(self, *args):
        try:
            name   = args[0]
            method = { "launch"  : self.launch,
                       "update"  : self.update,
                       "usage"   : self.usage,
                       "wait"    : self.wait,
                       "destroy" : self.destroy }[name]
        except IndexError:
            raise Err("Please choose a subcommand")
        except KeyError:
            raise Err("Subcommand %s is not valid for containerizers" % name)
        return method(*args[1:])

def methods():
    "Names of operations provided by containerizers, as a set."
    pairs = inspect.getmembers(Containerizer, predicate=inspect.ismethod)
    return set( k for k, _ in pairs if k[0:1] != "-" )

class Docker(Containerizer, _Struct):
    def __init__(self, workdir="/tmp/mesos-sandbox",
                       softlink_root="/tmp",
                       shared_dir="fs",
                       optimistic_unpack=True,
                       container_settings=deimos.config.Containers()):
        _Struct.__init__(self, workdir=workdir,
                               softlink_root=softlink_root,
                               shared_dir=shared_dir,
                               optimistic_unpack=optimistic_unpack,
                               config=container_settings)
    def launch(self, container_id, *args):
        log.info(" ".join([container_id] + list(args)))
        install_signal_handler(self.destroy, [container_id],
                               signal.SIGINT, signal.SIGTERM)
        mesos_directory()
        task = protos.TaskInfo()
        task.ParseFromString(sys.stdin.read())
        url, options = self.config.override(*container(task))
        pre, image = url.split("docker:///")
        if pre != "":
            raise Err("URL '%s' is not a valid docker:// URL!" % url)
        if image == "":
            image = deimos.docker.matching_image_for_host()
        docker_name = container_id_as_docker_name(container_id)
        run_options = ["--name", docker_name]

        place_uris(task, self.shared_dir, self.optimistic_unpack)
        run_options += ["-w", self.workdir]

        # Docker requires an absolute path to a source filesystem, separated
        # from the bind path in the container with a colon, but the absolute
        # path to the Mesos sandbox might have colons in it (TaskIDs with
        # timestamps can cause this situation). So we create a soft link to it
        # and mount that.
        sandbox_softlink = self.sandbox_softlink(docker_name, setup=True)
        run_options += ["-v", "%s:%s" % (sandbox_softlink, self.workdir)]

        cpus, mems = cpu_and_mem(task)
        env = [(_.name, _.value) for _ in task.command.environment.variables]
        run_options += options

        # We need to wrap the call to Docker in a call to the Mesos executor
        # if no executor is passed as part of the task. We need to pass the
        # MESOS_* environment variables in to the container if we're going to
        # start an executor.
        if needs_executor_wrapper(task):
            if not(len(args) > 1 and args[0] == "--mesos-executor"):
                raise Err("This task needs --mesos-executor to be set!")
            runner_argv = [args[1]]
        else:
            env += mesos_env() + [("MESOS_DIRECTORY", self.workdir)]
            runner_argv = []

        runner_argv += deimos.docker.run(run_options, image, argv(task),
                                        env=env, ports=ports(task),
                                        cpus=cpus, mems=mems)

        with open("stdout", "w") as o:        # This awkward double 'with' is a
            with open("stderr", "w") as e:    # concession to 2.6 compatibility
                call = deimos.cmd.in_sh(runner_argv, allstderr=False)
                try:
                    log.info(deimos.cmd.present(runner_argv))
                    runner = subprocess.Popen(call, stdout=o, stderr=e)
                    time.sleep(0.5)
                finally:
                    Run()(["rm", "-f", sandbox_softlink])
                proto_out(protos.PluggableStatus, message="launch/docker: ok")
                sys.stdout.close()      # Mark STDOUT as closed for Python code
                os.close(1)     # Use low-level call to close OS side of STDOUT
                runner_code = runner.wait()
        return runner_code
    def usage(self, container_id, *args):
        log.info(" ".join([container_id] + list(args)))
        name = container_id_as_docker_name(container_id)
        deimos.docker.await(name)
        cg = deimos.cgroups.CGroups(**deimos.docker.cgroups(name))
        if len(cg.keys()) == 0:
            raise Err("No CGroups found: %s" % container_id)
        try:
            proto_out(protos.ResourceStatistics,
                      timestamp             = time.time(),
                      mem_limit_bytes       = cg.memory.limit(),
                      cpus_limit            = cg.cpu.limit(),
                      cpus_user_time_secs   = cg.cpuacct.user_time(),
                      cpus_system_time_secs = cg.cpuacct.system_time(),
                      mem_rss_bytes         = cg.memory.rss())
        except AttributeError as e:
            log.error("Missing CGroup!")
            raise e
        return 0
    def destroy(self, container_id, *args):
        log.info(" ".join([container_id] + list(args)))
        name = container_id_as_docker_name(container_id)
        deimos.docker.await(name)
        for argv in [deimos.docker.stop(name), deimos.docker.rm(name)]:
            try:
                Run()(argv)
            except subprocess.CalledProcessError as e:
                log.error("Non-zero exit (%d): %r", e.returncode, argv)
                return e.returncode
        if not sys.stdout.closed:
            # If we're called as part of the signal handler set up by launch,
            # STDOUT is probably closed already. Writing the Protobuf would
            # only result in a bevy of error messages.
            proto_out(protos.PluggableStatus, message="destroy/docker: ok")
        return 0
    def sandbox_softlink(self, docker_name, setup=False):
        link = os.path.join(self.softlink_root, "deimos-fs." + docker_name)
        if setup:
            source = os.path.abspath(self.shared_dir)
            Run()(["ln", "-s", source, link])
        return link


####################################################### Mesos interface helpers

def fetch_command(task):
    if task.HasField("executor"):
        return task.executor.command
    return task.command

def fetch_container(task):
    cmd = fetch_command(task)
    if cmd.HasField("container"):
        return cmd.container

def container(task):
    container = fetch_container(task)
    if container is not None:
        return container.image, list(container.options)
    return "docker:///", []

def argv(task):
    cmd = fetch_command(task)
    if cmd.HasField("value") and cmd.value != "":
        return ["sh", "-c", cmd.value]
    return []

def uris(task):
    return fetch_command(task).uris

def ports(task):
    resources = [ _.ranges.range for _ in task.resources if _.name == 'ports' ]
    ranges = [ _ for __ in resources for _ in __ ]
    # NB: Casting long() to int() so there's no trailing 'L' in later
    #     stringifications. Ports should only ever be shorts, anyways.
    ports = [ range(int(_.begin), int(_.end)+1) for _ in ranges ]
    return [ port for r in ports for port in r ]

def cpu_and_mem(task):
    cpu, mem = None, None
    for r in task.resources:
        if r.name == "cpus":
            cpu = str(int(r.scalar.value * 1024))
        if r.name == "mem":
            mem = str(int(r.scalar.value)) + "m"
    return (cpu, mem)

def needs_executor_wrapper(task):
    return not task.HasField("executor")

def container_id_as_docker_name(container_id):
    if re.match(r"^[a-zA-Z0-9.-]+$", container_id):
        return "mesos." + container_id
    encoded = "mesos." + base64.b16encode(container_id)
    msg = "Creating a safe Docker name for ContainerID %r -> %s"
    log.info(msg, container_id, encoded)
    return encoded

MESOS_ESSENTIAL_ENV = [ "MESOS_SLAVE_ID",     "MESOS_SLAVE_PID",
                        "MESOS_FRAMEWORK_ID", "MESOS_EXECUTOR_ID" ]

def mesos_env():
    env = os.environ.get
    return [ (k, env(k)) for k in MESOS_ESSENTIAL_ENV if env(k) ]

def mesos_directory():
    if not "MESOS_DIRECTORY" in os.environ: return
    work_dir = os.path.abspath(os.getcwd())
    task_dir = os.path.abspath(os.environ["MESOS_DIRECTORY"])
    if task_dir != work_dir:
        log.info("Changing directory to MESOS_DIRECTORY=%s", task_dir)
        os.chdir(task_dir)

def place_uris(task, directory, optimistic_unpack=False):
    cmd = deimos.cmd.Run()
    cmd(["mkdir", "-p", directory])
    for item in uris(task):
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
            cmd(["curl", "-sSfL", uri, "--output", f])
        except subprocess.CalledProcessError as e:
            log.warning("Failed while processing URI: %s",
                        deimos.cmd.escape(uri))
            continue
        if item.executable:
            os.chmod(f, 0755)
        if gen_unpack_cmd:
            cmd(gen_unpack_cmd(f, directory))

def unpacker(uri):
    if re.match(r"[.](t|tar[.])(bz2|xz|gz)$", uri):
        return lambda f, directory: ["tar", "-C", directory, "-xf", f]
    if re.match(r"[.]zip$", uri):
        return lambda f, directory: ["unzip", "-d", directory, f]


####################################################### IO & system interaction

def proto_out(cls, **properties):
    """
    With a Protobuf class and properies as keyword arguments, sets all the
    properties on a new instance of the class and serializes the resulting
    value to stdout.
    """
    obj = cls()
    for k, v in properties.iteritems():
        log.debug("%s.%s = %r", cls.__name__, k, v)
        setattr(obj, k, v)
    data = obj.SerializeToString()
    sys.stdout.write(data)
    sys.stdout.flush()

def install_signal_handler(f, args=[], *signals):
    def handler(signum, _):
        log.warning("Signal: " + str(signum))
        f(*args)
        os._exit(-signum)
    for _ in signals: signal.signal(_, handler)

