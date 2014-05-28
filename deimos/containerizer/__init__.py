import inspect
import logging
import os
import re
import threading

import deimos.cmd
from deimos.logger import log

class Containerizer(object):
    def __init__(self): pass
    def launch(self, *args): pass
    def update(self, *args): pass
    def usage(self, *args): pass
    def wait(self, *args): pass
    def destroy(self, *args): pass
    def recover(self, *args): pass
    def containers(self, *args): pass
    def __call__(self, *args):
        try:
            name   = args[0]
            method = { "launch"     : self.launch,
                       "update"     : self.update,
                       "usage"      : self.usage,
                       "wait"       : self.wait,
                       "destroy"    : self.destroy,
                       "recover"    : self.recover,
                       "containers" : self.containers }[name]
        except IndexError:
            raise Err("Please choose a subcommand")
        except KeyError:
            raise Err("Subcommand %s is not valid for containerizers" % name)
        return method(*args[1:])
    def _image(self, launchy):
        url, options = self.container_settings.override(*launchy.container)
        pre, image = url.split("docker:///")
        if pre != "":
            raise Err("URL '%s' is not a valid docker:// URL!" % url)
        if image == "":
            image = self.default_image(launchy)
        log.info("image  = %s", image)

        return image

    def watch_process(self, proc, args):
        if proc is None:
            return
        thread = threading.Thread(target=proc.wait)
        thread.start()
        thread.join(10)
        if thread.is_alive():
            log.warning(deimos.cmd.present(args, "SIGTERM after 10s"))
            proc.terminate()
        thread.join(1)
        if thread.is_alive():
            log.warning(deimos.cmd.present(args, "SIGKILL after 1s"))
            proc.kill()
        msg = deimos.cmd.present(args, proc.wait())
        if proc.wait() == 0:
            log.info(msg)
        else:
            log.warning(msg)


def methods():
    "Names of operations provided by containerizers, as a set."
    pairs = inspect.getmembers(Containerizer, predicate=inspect.ismethod)
    return set( k for k, _ in pairs if k[0:1] != "_" )

####################################################### Mesos interface helpers

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

