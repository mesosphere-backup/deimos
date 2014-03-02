#!/usr/bin/env python
from __future__ import absolute_import
import subprocess
import sys

import medea.config
import medea.containerizer
from medea.err import Err
from medea.logger import log


def cli(argv=None):
    if argv is None: argv = sys.argv
    sub = argv[1] if len(argv) > 1 else None

    if sub in ["-h", "--help", "help"]:
        print format_help()
        return 0

    if sub not in medea.containerizer.methods():
        print >>sys.stderr, format_help()
        print >>sys.stderr, "** Please specify a subcommand **".center(79)
        return 1

    _, docker, containers = medea.config.load_configuration()
    containerizer = medea.containerizer.Docker(container_settings=containers)
    medea.docker.options = docker.argv()

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
 USAGE: medea launch  <container-id> (--mesos-executor /a/path)? < taskInfo.pb
        medea usage   <container-id>
        medea destroy <container-id>

  Medea provides Mesos integration for Docker, allowing Docker to be used as
  an external containerizer.

  In the first form, launches a container based on the TaskInfo passed on
  standard in. In the second, reports on its usage, with a ResourceStatistics
  Protobuf. In the third form, shuts down the container.
""".strip("\n")

if __name__ == "__main__":
    sys.exit(cli(sys.argv))
