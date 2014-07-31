import errno
from fcntl import LOCK_EX, LOCK_NB, LOCK_SH, LOCK_UN
from itertools import takewhile, dropwhile
import logging
import os
import random
import re
import signal
import subprocess
import sys
import threading
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
from deimos.containerizer import *
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


class Docker(Containerizer, _Struct):

    def __init__(self, workdir="/tmp/mesos-sandbox",
                       state_root="/tmp/deimos",
                       shared_dir="fs",
                       optimistic_unpack=True,
                       hooks=deimos.config.Hooks(),
                       container_settings=deimos.config.Containers(),
                       index_settings=deimos.config.DockerIndex()):
        _Struct.__init__(self, workdir=workdir,
                               state_root=state_root,
                               shared_dir=shared_dir,
                               optimistic_unpack=optimistic_unpack,
                               hooks=hooks,
                               container_settings=container_settings,
                               index_settings=index_settings,
                               runner=None,
                               state=None)

    def launch(self, launch_pb, *args):
        log.info(" ".join(args))
        fork = False if "--no-fork" in args else True
        deimos.sig.install(self.log_signal)
        run_options = []
        launchy = deimos.mesos.Launch(launch_pb)
        state = deimos.state.State(self.state_root,
                                   mesos_id=launchy.container_id)
        state.push()
        lk_l = state.lock("launch", LOCK_EX)
        state.executor_id = launchy.executor_id
        state.push()
        state.ids()
        mesos_directory()  # Redundant?
        if launchy.directory:
            os.chdir(launchy.directory)
        # TODO: if launchy.user:
        #           os.seteuid(launchy.user)
        url, options = launchy.container
        options, trailing_argv = split_on(options, "//")
        url, options = self.container_settings.override(url, options)

        true_argv = launchy.argv if trailing_argv is None else trailing_argv

        image = self.determine_image(url, launchy)
        log.info("image  = %s", image)
        run_options += ["--sig-proxy"]
        run_options += ["--rm"]       # This is how we ensure container cleanup
        run_options += ["--cidfile", state.resolve("cid")]

        place_uris(launchy, self.shared_dir, self.optimistic_unpack)
        run_options += ["-w", self.workdir]

        # Docker requires an absolute path to a source filesystem, separated
        # from the bind path in the container with a colon, but the absolute
        # path to the Mesos sandbox might have colons in it (TaskIDs with
        # timestamps can cause this situation). So we create a soft link to it
        # and mount that.
        shared_full = os.path.abspath(self.shared_dir)
        sandbox_symlink = state.sandbox_symlink(shared_full)
        run_options += ["-v", "%s:%s" % (sandbox_symlink, self.workdir)]

        cpus, mems = launchy.cpu_and_mem
        env = launchy.env
        run_options += options

        # We need to wrap the call to Docker in a call to the Mesos executor
        # if no executor is passed as part of the task. We need to pass the
        # MESOS_* environment variables in to the container if we're going to
        # start an executor.
        observer_argv = None
        if launchy.needs_observer:
            # NB: The "@@docker@@" variant is a work around for Mesos's option
            # parser. There is a fix in the pipeline.
            observer_argv = [mesos_executor(), "--override",
                             deimos.path.me(), "observe", state.mesos_id]
            state.lock("observe", LOCK_EX | LOCK_NB)     # Explanation of Locks
            # When the observer is running, we would like its call to
            # observe() to finish before all the wait(); and we'd like the
            # observer to have a chance to report TASK_FINISHED before the
            # calls to wait() report their results (which would result in a
            # TASK_FAILED).
            #
            # For this reason, we take the "observe" lock in launch(), before
            # we call the observer and before releasing the "launch" or "wait"
            # locks.
            #
            # Calls to observe() actually skip locking "observe"; but wait()
            # calls must take this lock. The "observe" lock is held by
            # launch() until the observer executor completes, at which point
            # we can be reasonably sure its status was propagated to the Mesos
            # slave.
        else:
            env += mesos_env() + [("MESOS_DIRECTORY", self.workdir)]

        # Flatten our env list of tuples into dictionary object for Popen
        popen_env = dict(env)

        self.place_dockercfg()

        runner_argv = deimos.docker.run(run_options, image, true_argv,
                                        env=env, ports=launchy.ports,
                                        cpus=cpus, mems=mems)

        log_mesos_env(logging.DEBUG)

        observer = None
        with open("stdout", "w") as o:        # This awkward multi 'with' is a
            with open("stderr", "w") as e:    # concession to 2.6 compatibility
                with open(os.devnull) as devnull:
                    log.info(deimos.cmd.present(runner_argv))

                    onlaunch = self.hooks.onlaunch
                    # test for default configuration (empty list)
                    if onlaunch:
                        # We're going to catch all exceptions because it's not
                        # in scope for Deimos to stack trace on a hook error
                        try:
                            subprocess.Popen(onlaunch, stdin=devnull,
                                             stdout=devnull,
                                             stderr=devnull,
                                             env=popen_env)
                        except Exception as e:
                            log.warning("onlaunch hook failed with %s" % e)

                    self.runner = subprocess.Popen(runner_argv, stdin=devnull,
                                                                stdout=o,
                                                                stderr=e)
                    state.pid(self.runner.pid)
                    state.await_cid()
                    state.push()
                    lk_w = state.lock("wait", LOCK_EX)
                    lk_l.unlock()
                    if fork:
                        pid = os.fork()
                        if pid is not 0:
                            state.ids()
                            log.info("Forking watcher into child...")
                            return
                    state.ids()
                    if observer_argv is not None:
                        log.info(deimos.cmd.present(observer_argv))
                        call = deimos.cmd.in_sh(observer_argv, allstderr=False)
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
            if p is None:
                continue
            thread = threading.Thread(target=p.wait)
            thread.start()
            thread.join(10)
            if thread.is_alive():
                log.warning(deimos.cmd.present(arr, "SIGTERM after 10s"))
                p.terminate()
            thread.join(1)
            if thread.is_alive():
                log.warning(deimos.cmd.present(arr, "SIGKILL after 1s"))
                p.kill()
            msg = deimos.cmd.present(arr, p.wait())
            if p.wait() == 0:
                log.info(msg)
            else:
                log.warning(msg)

        with open(os.devnull) as devnull:
            ondestroy = self.hooks.ondestroy
            if ondestroy:
                # Deimos shouldn't care if the hook fails. The hook should implement its own error handling
                try:
                    subprocess.Popen(ondestroy, stdin=devnull,
                                     stdout=devnull,
                                     stderr=devnull,
                                     env=popen_env)
                except Exception as e:
                    log.warning("ondestroy hook failed with %s" % e)

        return state.exit()

    def update(self, update_pb, *args):
        log.info(" ".join(args))
        log.info("Update is a no-op for Docker...")

    def usage(self, usage_pb, *args):
        log.info(" ".join(args))
        container_id = usage_pb.container_id.value
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
            recordio.write(ResourceStatistics,
                           timestamp=time.time(),
                           mem_limit_bytes=cg.memory.limit(),
                           cpus_limit=cg.cpu.limit(),
                           # cpus_user_time_secs   = cg.cpuacct.user_time(),
                           # cpus_system_time_secs = cg.cpuacct.system_time(),
                           mem_rss_bytes=cg.memory.rss())
        except AttributeError as e:
            log.error("Missing CGroup!")
            raise e
        return 0

    def observe(self, *args):
        log.info(" ".join(args))
        state = deimos.state.State(self.state_root, mesos_id=args[0])
        self.state = state
        deimos.sig.install(self.stop_docker_and_resume)
        state.await_launch()
        try:  # Take the wait lock to block calls to wait()
            state.lock("wait", LOCK_SH, seconds=None)
        except IOError as e:                       # Allows for signal recovery
            if e.errno != errno.EINTR:
                raise e
            state.lock("wait", LOCK_SH, seconds=1)
        if state.exit() is not None:
            return state.exit()
        raise Err("Wait lock is not held nor is exit file present")

    def wait(self, wait_pb, *args):
        log.info(" ".join(args))
        container_id = wait_pb.container_id.value
        state = deimos.state.State(self.state_root, mesos_id=container_id)
        self.state = state
        deimos.sig.install(self.stop_docker_and_resume)
        state.await_launch()
        try:  # Wait for the observe lock so observe completes first
            state.lock("observe", LOCK_SH, seconds=None)
            state.lock("wait", LOCK_SH, seconds=None)
        except IOError as e:                       # Allows for signal recovery
            if e.errno != errno.EINTR:
                raise e
            state.lock("observe", LOCK_SH, seconds=1)
            state.lock("wait", LOCK_SH, seconds=1)
        termination = (state.exit() if state.exit() is not None else 64) << 8
        recordio.write(Termination,
                       killed=False,
                       message="",
                       status=termination)
        if state.exit() is not None:
            return state.exit()
        raise Err("Wait lock is not held nor is exit file present")

    def destroy(self, destroy_pb, *args):
        log.info(" ".join(args))
        container_id = destroy_pb.container_id.value
        state = deimos.state.State(self.state_root, mesos_id=container_id)
        state.await_launch()
        lk_d = state.lock("destroy", LOCK_EX)
        if state.exit() is None:
            Run()(deimos.docker.stop(state.cid()))
        else:
            log.info("Container is stopped")
        return 0

    def containers(self, *args):
        log.info(" ".join(args))
        data = Run(data=True)(deimos.docker.docker("ps", "--no-trunc", "-q"))
        mesos_ids = []
        for line in data.splitlines():
            cid = line.strip()
            state = deimos.state.State(self.state_root, docker_id=cid)
            if not state.exists():
                continue
            try:
                state.lock("wait", LOCK_SH | LOCK_NB)
            except deimos.flock.Err:     # LOCK_EX held, so launch() is running
                mesos_ids += [state.mesos_container_id()]
        containers = Containers()
        for mesos_id in mesos_ids:
            container = containers.containers.add()
            container.value = mesos_id
        recordio.writeProto(containers)
        return 0

    def log_signal(self, signum):
        pass

    def stop_docker_and_resume(self, signum):
        if self.state is not None and self.state.cid() is not None:
            cid = self.state.cid()
            log.info("Trying to stop Docker container: %s", cid)
            try:
                Run()(deimos.docker.stop(cid))
            except subprocess.CalledProcessError:
                pass
            return deimos.sig.Resume()

    def determine_image(self, url, launchy):
        opts = dict(self.container_settings.image.items(onlyset=True))
        if "default" in opts:
            default = url_to_image(opts["default"])
        else:
            default = self.image_from_system_context(launchy)
        image = url_to_image(url)
        return default if image == "" else image

    def image_from_system_context(self, launchy):
        opts = dict(self.index_settings.items(onlyset=True))
        if "account_libmesos" in opts:
            if not launchy.needs_observer:
                opts["account"] = opts["account_libmesos"]
            del opts["account_libmesos"]
        if "dockercfg" in opts:
            del opts["dockercfg"]
        return deimos.docker.matching_image_for_host(**opts)

    def place_dockercfg(self):
        dockercfg = self.index_settings.dockercfg
        if dockercfg is not None:
            log.info("Copying to .dockercfg: %s" % dockercfg)
            Run()(["cp", dockercfg, ".dockercfg"])

def url_to_image(url):
    pre, image = re.split(r"^docker:///?", url)
    if pre != "":
        raise Err("URL '%s' is not a valid docker:// URL!" % url)
    return image

def split_on(iterable, element):
    preceding = list(takewhile(lambda _: _ != element, iterable))
    following = list(dropwhile(lambda _: _ != element, iterable))
    return preceding, (following[1:] if len(following) > 0 else None)
