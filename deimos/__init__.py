#!/usr/bin/env python
import os
import signal
import subprocess
import sys

import deimos.config
import deimos.containerizer
from deimos.err import Err
import deimos.flock
from deimos.logger import log
import deimos.sig


def cli(argv=None):
    deimos.sig.install(lambda _: None)
    if argv is None: argv = sys.argv
    sub = argv[1] if len(argv) > 1 else None

    if sub in ["-h", "--help", "help"]:
        print format_help()
        return 0

    conf = deimos.config.load_configuration()

    if sub == "locks":
        deimos.flock.lock_browser(os.path.join(conf.state.root, "mesos"))
        return 0

    if sub not in deimos.containerizer.methods():
        print >>sys.stderr, format_help()
        print >>sys.stderr, "** Please specify a subcommand **".center(79)
        return 1

    deimos.docker.options = conf.docker.argv()
    containerizer = deimos.containerizer.Docker(
        container_settings=conf.containers,
        optimistic_unpack=conf.uris.unpack,
        state_root=conf.state.root
    )

    try:
        result = containerizer(*argv[1:])
        if result is not None:
            if isinstance(result, int):
                return result
            if isinstance(result, str):
                sys.stdout.write(result)
            else:
                for item in result:
                    sys.stdout.write(str(item) + "\n")
    except Err as e:
        log.error("%s.%s: %s", type(e).__module__, type(e).__name__, str(e))
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
 USAGE: deimos launch  <container-id> (--executor /a/path)? < taskInfo.pb
        deimos usage   <container-id> > resources.pb
        deimos destroy <container-id>
        deimos wait --docker <docker-id>
        deimos locks

  Deimos provides Mesos integration for Docker, allowing Docker to be used as
  an external containerizer.

 deimos launch <container-id> (--executor /a/path)?

  Launches a container and runs the executor or command specified in the
  TaskInfo, passed in on standard in. The path passed as the --executor option
  is used when the task specifies a command and no actual executor -- the
  passed program is run as an "observer" to communicate task status to the
  local Mesos node.

 deimos usage <container-id>

  Generates a protobuf description of the resources used by the container.

 deimos destroy <container-id>

  Shuts down the specified container.

 deimos wait --docker <docker-id>

  Waits for a particular Docker CID to exit and records the result in the
  state directory, returning an appropriate exit code.

 deimos locks

  List file locks taken by Deimos, associating each file with a PID, an inode,
  and a lock level. The same file may appear multiple times.

""".strip("\n")

if __name__ == "__main__":
    sys.exit(cli(sys.argv))

