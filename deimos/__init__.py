#!/usr/bin/env python
from __future__ import absolute_import
import subprocess
import sys

import deimos.config
import deimos.containerizer
from deimos.err import Err
from deimos.logger import log


def cli(argv=None):
    if argv is None: argv = sys.argv
    sub = argv[1] if len(argv) > 1 else None

    if sub in ["-h", "--help", "help"]:
        print format_help()
        return 0

    if sub not in deimos.containerizer.methods():
        print >>sys.stderr, format_help()
        print >>sys.stderr, "** Please specify a subcommand **".center(79)
        return 1

    _, docker, containers, uris = deimos.config.load_configuration()
    deimos.docker.options = docker.argv()
    containerizer = deimos.containerizer.Docker(container_settings=containers,
                                                optimistic_unpack=uris.unpack)

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
 USAGE: deimos launch  <container-id> (--mesos-executor /a/path)? < taskInfo.pb
        deimos usage   <container-id>
        deimos destroy <container-id>

  Deimos provides Mesos integration for Docker, allowing Docker to be used as
  an external containerizer.

  In the first form, launches a container based on the TaskInfo passed on
  standard in. In the second, reports on its usage, with a ResourceStatistics
  Protobuf. In the third form, shuts down the container.
""".strip("\n")

if __name__ == "__main__":
    sys.exit(cli(sys.argv))
