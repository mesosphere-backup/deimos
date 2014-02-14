import inspect
import sys

def launch(container_id):
    pass

def update(container_id):
    pass

def usage(container_id):
    pass

def wait(container_id):
    pass

def destroy(container_id):
    pass

# Internal function listing used by CLI.
_mod = sys.modules[__name__]
_subcommands = dict( (k, v) for k, v in _mod.__dict__.items()
                             if k[0] != "_" and inspect.isfunction(v) and
                                                inspect.getmodule(v) == _mod )

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

def cli(argv):
    sub = argv[1] if len(argv) > 1 else None

    if sub in ["-h", "--help", "help"]:
        print format_help()
        sys.exit(0)

    f = _subcommands.get(sub)

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

if __name__ == "__main__":
    cli(sys.argv)
