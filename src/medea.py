#!/usr/bin/env python
import inspect
import re
import subprocess
import sys

try:
    import mesos_pb2 as protos                     # Prefer system installation
except:
    import medea.mesos_pb2 as protos


def launch(container_id, *args):
    task = protos.TaskInfo.ParseFromString(sys.stdin.read())
    (url, options) = container(task)
    pre, image = url.split("docker:///")[1]
    if pre != "":
        raise Err("URL '%s' is not a valid docker:// URL!" % url)
    if image == "":
        image = matching_docker_for_host()
    docker_name = container_id_as_docker_name(container_id)

    run_options = ["--name", docker_name]
    for r in task.resources:
        if r.name == "cpus":
            run_options += [ "-c", str(int(r.scalar.value * 256)) ]
        if r.name == "mem":
            run_options += [ "-m", str(int(r.scalar.value)) + "m" ]
        # TODO: Handle ports in here?
    for k, v in os.environ.items():
        run_options += [ "-e", "%s=%s" % (k,v) ]

    daemon_argv = docker_private_daemon()
    runner_argv = docker_run(run_options + options, image, argv(task))
    if needs_executor_wrapper(task):
        if len(args) > 1 and args[0] == "--mesos-executor":
            runner_argv = [args[1]] + runner_argv

    daemon = subprocess.Popen(in_sh(daemon_argv))
    try:
        runner = subprocess.Popen(in_sh(runner_argv))
        runner_code = runner.wait()
    finally:
        daemon.terminate()
    daemon_code = daemon.wait()
    if daemon_code != 0:
        msg = "!! Private daemon exited with an error (%d)" % daemon_code
        print >>sys.stderr, msg
    return runner_code

def update(container_id):
    pass

def usage(container_id):
    pass

def wait(container_id):
    name = container_id_as_docker_name(container_id)
    return subprocess.call(in_sh(["docker", "wait", name]))

def destroy(container_id):
    exit = 0
    name = container_id_as_docker_name(container_id)
    for argv in [["docker", "stop", "-t=2", name], ["docker", "rm", name]]:
        try: subprocess.check_call(in_sh(argv))
        except subprocess.CalledProcessError as e:
            exit = e.returncode
            print >>sys.stderr, "!! Bad exit code (%d):" % exit, argv
    return exit


def docker_run(options, image, command=[]):
    socket = docker_private_socket()
    return ["docker", socket, "run"] + options + [image] + command

def docker_private_dameon():
    pidfile = "--pidfile=" + os.getcwd() + "/docker.pid"
    socket = docker_private_socket()
    return ["docker", "-d", socket, pidfile]

def docker_private_socket():
    return "--host=unix://" + os.getcwd() + "/docker.sock"

def in_sh(argv):
    """
    Provides better error messages in case of file not found or permission
    denied. Note that this has nothing at all to do with shell=True, since
    quoting prevents the shell from interpreting any arguments.
    """
    return ["/bin/sh", "-c", 'exec "$@"', "sh"] + argv


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
        return (container.image, container.options)
    return ("docker:///", [])

def argv(task):
    cmd = fetch_command(task)
    if cmd.HasField("value") and value != "":
        return ["sh", "-c", cmd.value]
    return []

def needs_executor_wrapper(task):
    return not task.HasField("executor")

def matching_docker_for_host():
    return subprocess.check_output(["bash", "-c", """
        [[ ! -s /etc/os-release ]] ||
        ( source /etc/os-release && tr A-Z a-z <<<"$ID":"$VERSION_ID" )
    """])

def container_id_as_docker_name(container_id):
    s = container_id
    r = s if re.match(r"^[a-zA-Z.-]+$", s) else "mesos-" + base64.b16encode(s)
    return r


def cli(argv):
    sub = argv[1] if len(argv) > 1 else None

    if sub in ["-h", "--help", "help"]:
        print format_help()
        sys.exit(0)

    f = { "launch":  launch,
          "update":  update,
          "usage":   usage,
          "wait":    wait,
          "destroy": destroy }.get(sub)

    if f is None:
        print >>sys.stderr, format_help()
        print >>sys.stderr, "** Please specify a subcommand **".center(79)
        sys.exit(1)

    result = f(*argv[2:])
    if result is not None:
        if isinstance(result, int):
            sys.exit(result)
        if isinstance(result, str):
            sys.stdout.write(result)
        else:
            for item in result:
                sys.stdout.write(str(item) + "\n")

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


class Err(RuntimeError):
    pass

# This try block is here to upgrade functionality available the subprocess
# module for older versions of Python. As last as 2.6, subprocess did not have
# the check_output function.
try:
    subprocess.check_output
except:
    def check_output(*args):
        p = subprocess.Popen(stdout=subprocess.PIPE, *args)
        stdout = p.communicate()[0]
        exitcode = p.wait()
        if exitcode:
            raise subprocess.CalledProcessError(exitcode, args[0])
        return stdout
    subprocess.check_output = check_output


if __name__ == "__main__":
    cli(sys.argv)

