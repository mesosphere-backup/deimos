import base64
import errno
from fcntl import LOCK_EX, LOCK_NB, LOCK_SH, LOCK_UN
import inspect
import logging
import os
import random
import re
import signal
import subprocess
import sys
import time

import google.protobuf

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
import deimos.path
from deimos._struct import _Struct
import deimos.state
import deimos.sig


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
    return set( k for k, _ in pairs if k[0:1] != "_" )

class Docker(Containerizer, _Struct):
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
    def launch(self, container_id, *args):
        log.info(" ".join([container_id] + list(args)))
        deimos.sig.install(self.sig_proxy)
        run_options = []
        state = deimos.state.State(self.state_root, mesos_id=container_id)
        state.push()
        lk_l = state.lock("launch", LOCK_EX)
        mesos_directory()
        task = protos.TaskInfo()
        task.ParseFromString(sys.stdin.read())
        for line in proto_lines(task):
            log.debug(line)
        state.executor_id = executor_id(task)
        state.push()
        state.ids()
        url, options = self.container_settings.override(*container(task))
        pre, image = url.split("docker:///")
        if pre != "":
            raise Err("URL '%s' is not a valid docker:// URL!" % url)
        if image == "":
            image = self.default_image(task)
        log.info("image  = %s", image)
        run_options += [ "--sig-proxy" ]
        run_options += [ "--rm" ]     # This is how we ensure container cleanup
        run_options += [ "--cidfile", state.resolve("cid") ]

        place_uris(task, self.shared_dir, self.optimistic_unpack)
        run_options += [ "-w", self.workdir ]

        # Docker requires an absolute path to a source filesystem, separated
        # from the bind path in the container with a colon, but the absolute
        # path to the Mesos sandbox might have colons in it (TaskIDs with
        # timestamps can cause this situation). So we create a soft link to it
        # and mount that.
        shared_full = os.path.abspath(self.shared_dir)
        sandbox_symlink = state.sandbox_symlink(shared_full)
        run_options += [ "-v", "%s:%s" % (sandbox_symlink, self.workdir) ]

        cpus, mems = cpu_and_mem(task)
        env = [(_.name, _.value) for _ in task.command.environment.variables]
        run_options += options

        # We need to wrap the call to Docker in a call to the Mesos executor
        # if no executor is passed as part of the task. We need to pass the
        # MESOS_* environment variables in to the container if we're going to
        # start an executor.
        observer_argv = None
        if needs_executor_wrapper(task):
            options = ["--mesos-executor", "--observer"]
            if not(len(args) > 1 and args[0] in options):
                raise Err("Task %s needs --observer to be set!" % state.eid())
            observer_argv = list(args[1:]) + [ deimos.path.me(),
                                               "wait", "--docker" ]
        else:
            env += mesos_env() + [("MESOS_DIRECTORY", self.workdir)]

        runner_argv = deimos.docker.run(run_options, image, argv(task),
                                        env=env, ports=ports(task),
                                        cpus=cpus, mems=mems)

        log_mesos_env(logging.DEBUG)

        observer = None
        with open("stdout", "w") as o:        # This awkward multi 'with' is a
            with open("stderr", "w") as e:    # concession to 2.6 compatibility
                with open(os.devnull) as devnull:
                    log.info(deimos.cmd.present(runner_argv))
                    self.runner = subprocess.Popen(runner_argv, stdin=devnull,
                                                                stdout=o,
                                                                stderr=e)
                    state.pid(self.runner.pid)
                    state.await_cid()
                    state.push()
                    lk_w = state.lock("wait", LOCK_EX)
                    lk_l.unlock()
                    state.ids()
                    proto_out(protos.ExternalStatus, message="launch: ok")
                    sys.stdout.close()  # Mark STDOUT as closed for Python code
                    os.close(1) # Use low-level call to close OS side of STDOUT
                    if observer_argv is not None:
                        observer_argv += [state.cid()]
                        log.info(deimos.cmd.present(observer_argv))
                        call = deimos.cmd.in_sh(observer_argv)
                        # TODO: Collect these leaking file handles.
                        obs_out = open(state.resolve("observer.out"), "w+")
                        obs_err = open(state.resolve("observer.err"), "w+")
                        # If the Mesos executor sees LIBPROCESS_PORT=0 (which
                        # is passed by the slave) there are problems when it
                        # attempts to bind. ("Address already in use").
                        # Purging both LIBPROCESS_* net variables, to be safe.
                        for v in ["LIBPROCESS_PORT", "LIBPROCESS_IP"]:
                            if v in os.environ:
                                del os.environ[v]
                        observer = subprocess.Popen(call, stdin=devnull,
                                                          stdout=obs_out,
                                                          stderr=obs_err,
                                                          close_fds=True)
        data = Run(data=True)(deimos.docker.wait(state.cid()))
        state.exit(data)
        lk_w.unlock()
        for p, arr in [(self.runner, runner_argv), (observer, observer_argv)]:
            if p is None or p.wait() == 0:
                continue
            log.warning(deimos.cmd.present(arr, p.wait()))
        return state.exit()
    def usage(self, container_id, *args):
        log.info(" ".join([container_id] + list(args)))
        state = deimos.state.State(self.state_root, mesos_id=container_id)
        state.await_launch()
        state.ids()
        if state.cid() is None:
            log.info("Container not started?")
            return 0
        if state.exit() is not None:
            log.info("Container is stopped")
            return 0
        cg = deimos.cgroups.CGroups(**deimos.docker.cgroups(state.cid()))
        if len(cg.keys()) == 0:
            log.info("Container has no CGroups...already stopped?")
            return 0
        try:
            proto_out(protos.ResourceStatistics,
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
        log.info(" ".join(list(args)))
        if list(args[0:1]) != ["--docker"]:
            return      # We rely on the Mesos default wait strategy in general
        # In Docker mode, we use Docker wait to wait for the container and
        # then exit with the returned exit code. The passed in ID should be a
        # Docker CID, not a Mesos container ID.
        state = deimos.state.State(self.state_root, docker_id=args[1])
        self.state = state
        deimos.sig.install(self.stop_docker_and_resume)
        state.await_launch()
        try:
            state.lock("wait", LOCK_SH, seconds=None)
        except IOError as e:                       # Allows for signal recovery
            if e.errno != errno.EINTR:
                raise e
            state.lock("wait", LOCK_SH, 1)
        if state.exit() is not None:
            return state.exit()
        raise Err("Wait lock is not held nor is exit file present")
    def destroy(self, container_id, *args):
        log.info(" ".join([container_id] + list(args)))
        state = deimos.state.State(self.state_root, mesos_id=container_id)
        state.await_launch()
        lk_d = state.lock("destroy", LOCK_EX)
        if state.exit() is not None:
            Run()(deimos.docker.stop(state.cid()))
        else:
            log.info("Container is stopped")
        if not sys.stdout.closed:
            # If we're called as part of the signal handler set up by launch,
            # STDOUT is probably closed already. Writing the Protobuf would
            # only result in a bevy of error messages.
            proto_out(protos.ExternalStatus, message="destroy: ok")
        return 0
    def sig_proxy(self, signum):
        if self.runner is not None:
            self.runner.send_signal(signum)
    def stop_docker_and_resume(self, signum):
        if self.state is not None and self.state.cid() is not None:
            cid = self.state.cid()
            log.info("Trying to stop Docker container: %s", cid)
            try:
                Run()(deimos.docker.stop(cid))
            except subprocess.CalledProcessError:
                pass
            return deimos.sig.Resume()
    def default_image(self, task):
        opts = dict(self.index_settings.items(onlyset=True))
        if "account_libmesos" in opts:
            if not needs_executor_wrapper(task):
                opts["account"] = opts["account_libmesos"]
            del opts["account_libmesos"]
        return deimos.docker.matching_image_for_host(**opts)

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

def executor_id(task):
    if needs_executor_wrapper(task):
        return task.task_id.value
    else:
        return task.executor.executor_id.value

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

MESOS_ESSENTIAL_ENV = [ "MESOS_SLAVE_ID",     "MESOS_SLAVE_PID",
                        "MESOS_FRAMEWORK_ID", "MESOS_EXECUTOR_ID" ]

def mesos_env():
    env = os.environ.get
    return [ (k, env(k)) for k in MESOS_ESSENTIAL_ENV if env(k) ]

def log_mesos_env(level=logging.INFO):
    for k, v in os.environ.items():
        if k.startswith("MESOS_") or k.startswith("LIBPROCESS_"):
            log.log(level, "%s=%s" % (k, v))

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
        if gen_unpack_cmd is not None:
            log.info("Unpacking %s" % f)
            cmd(gen_unpack_cmd(f, directory))
            cmd(["rm", "-f", f])

def unpacker(uri):
    if re.search(r"[.](t|tar[.])(bz2|xz|gz)$", uri):
        return lambda f, directory: ["tar", "-C", directory, "-xf", f]
    if re.search(r"[.]zip$", uri):
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

def proto_lines(proto):
    s = google.protobuf.text_format.MessageToString(proto)
    return s.strip().split("\n")

