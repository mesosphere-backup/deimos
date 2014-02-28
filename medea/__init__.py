#!/usr/bin/env python
from __future__ import absolute_import
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
except: import medea.mesos_pb2 as protos

import medea.cmd
import medea.cgroups
import medea.config
import medea.docker
from medea.err import *
import medea.logger
from medea.logger import log


####################################################### Containerizer interface

def launch(container_id, *args):
    cmd = medea.cmd.Run()
    log.info(" ".join([container_id] + list(args)))
    install_signal_handler(container_id, signal.SIGINT, signal.SIGTERM)
    mesos_directory()
    task = protos.TaskInfo()
    task.ParseFromString(sys.stdin.read())
    (url, options) = container(task)
    pre, image = url.split("docker:///")
    if pre != "":
        raise Err("URL '%s' is not a valid docker:// URL!" % url)
    if image == "":
        image = matching_docker_for_host()
    docker_name = container_id_as_docker_name(container_id)
    run_options = [ "--name", docker_name ]

    place_uris(task, "fs")
    sandbox_mountpoint = "/tmp/mesos-sandbox/"
    run_options += [ "-w", sandbox_mountpoint ]

    # Docker requires an absolute path to a source filesystem, separated from
    # the bind path in the container with a colon, but the absolute path to
    # the Mesos sandbox might have colons in it (TaskIDs with timestamps can
    # cause this situation). So we create a soft link to it and mount that.
    sandbox_softlink = "/tmp/medes-fs." + docker_name
    cmd(["ln", "-s", os.path.abspath("fs"), sandbox_softlink])
    run_options += [ "-v", "%s:%s" % (sandbox_softlink, sandbox_mountpoint) ]

    cpus, mems = cpu_and_mem(task)
    env = [(_.name, _.value) for _ in task.command.environment.variables]
    run_options += options

    # We need to wrap the call to Docker in a call to the Mesos executor if no
    # executor is passed as part of the task. We need to pass the MESOS_*
    # environment variables in to the container if we're going to start an
    # executor.
    if needs_executor_wrapper(task):
        if not(len(args) > 1 and args[0] == "--mesos-executor"):
            raise Err("This task needs --mesos-executor to be set!")
        runner_argv = [args[1]]
    else:
        env += mesos_env() + [("MESOS_DIRECTORY", sandbox_mountpoint)]
        runner_argv = []

    runner_argv += medea.docker.run(run_options, image, argv(task), env=env,
                                    ports=ports(task), cpus=cpus, mems=mems)

    with open("stdout", "w") as o:            # This awkward double 'with' is a
        with open("stderr", "w") as e:        # concession to 2.6 compatibility
            call = medea.cmd.in_sh(runner_argv, allstderr=False)
            try:
                log.info(medea.cmd.present(runner_argv))
                runner = subprocess.Popen(call, stdout=o, stderr=e)
                time.sleep(0.1)
            finally:
                cmd(["rm", "-f", sandbox_softlink])
            proto_out(protos.PluggableStatus, message="launch/docker: ok")
            sys.stdout.close()          # Mark STDOUT as closed for Python code
            os.close(1)         # Use low-level call to close OS side of STDOUT
            runner_code = runner.wait()
    return runner_code

def usage(container_id, *args):
    log.info(" ".join([container_id] + list(args)))
    name = container_id_as_docker_name(container_id)
    medea.docker.await(name)
    cg = medea.cgroups.CGroups(**medea.docker.cgroups(name))
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

def destroy(container_id, *args):
    cmd = medea.cmd.Run()
    log.info(" ".join([container_id] + list(args)))
    name = container_id_as_docker_name(container_id)
    medea.docker.await(name)
    for argv in [medea.docker.stop(name), medea.docker.rm(name)]:
        try:
            cmd(argv)
        except subprocess.CalledProcessError as e:
            log.error("Non-zero exit (%d): %r", e.returncode, argv)
            return e.returncode
    if not sys.stdout.closed:
        # If we're called as part of the signal handler set up by launch,
        # STDOUT is probably closed already. Writing the Protobuf would only
        # result in a bevy of error messages.
        proto_out(protos.PluggableStatus, message="destroy/docker: ok")
    return 0


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
        return (container.image, list(container.options))
    return ("docker:///", [])

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

def place_uris(task, directory):
    cmd = medea.cmd.Run()
    cmd(["mkdir", "-p", directory])
    for item in uris(task):
        uri = item.value
        log.info("Retrieving URI: %s", medea.cmd.escape([uri]))
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
            log.warning("Non-zero exit (%d): %r", e.returncode, argv)
            log.warning("Failed while processing URI: %r", uri)
            continue
        if item.executable:
            os.chmod(f, 0755)


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

def matching_docker_for_host():
    return medea.cmd.Run(data=True)(["bash", "-c", """
        [[ ! -s /etc/os-release ]] ||
        ( source /etc/os-release && tr A-Z a-z <<<"$ID":"$VERSION_ID" )
    """]).strip()

def install_signal_handler(container_id, *signals):
    def handler(signum, _):
        log.warning("Signal: " + str(signum))
        destroy(container_id)
        os._exit(-signum)
    for _ in signals: signal.signal(_, handler)


##################################################### CLI, errors, Python stuff

def cli(argv=None):
    if argv is None: argv = sys.argv

    sub = argv[1] if len(argv) > 1 else None

    if sub in ["-h", "--help", "help"]:
        print format_help()
        return 0

    f = { "launch"  : launch,
          "update"  : lambda *args: None,
          "usage"   : usage,
          "wait"    : lambda *args: None,
          "destroy" : destroy }.get(sub)

    if f is None:
        print >>sys.stderr, format_help()
        print >>sys.stderr, "** Please specify a subcommand **".center(79)
        return 1

    _, _, containers = medea.config.load_configuration()

    try:
        result = f(*argv[2:])
        if result is not None:
            if isinstance(result, int):
                return result
            if isinstance(result, str):
                sys.stdout.write(result)
            else:
                for item in result:
                    sys.stdout.write(str(item) + "\n")
    except Err as e:
        log.error(str(e))
        return 4
    except subprocess.CalledProcessError as e:
        log.error(str(e))
        return 4
    except Exception:
        log.exception("Unhandled failure in %s", sub)
        return 8
    return 0

def format_help():
    return """
 USAGE: medea launch  <container-id> (--mesos-executor /a/path)? < taskInfo.pb
        medea update  <container-id> < resources.pb
        medea usage   <container-id>
        medea wait    <container-id>
        medea destroy <container-id>

  Medea provides Mesos integration for Docker, allowing Docker to be used as
  an external containerizer.

  In the first form, launches a container based on the TaskInfo passed on
  standard in. In the second form, updates a container's resources. The
  remaining forms are effectively no-ops, returning 0 and sending back 0 bytes
  of data, allowing Mesos to use its default behaviour.
""".strip("\n")

if __name__ == "__main__":
    sys.exit(cli(sys.argv))
