from deimos._struct import _Struct


class Launch(_Struct):

    def __init__(self, proto):
        underlying = LaunchProto(proto)
        self._underlying = underlying
        _Struct.__init__(self, executor_id=underlying.executor_id(),
                               container_id=underlying.container_id(),
                               container=underlying.container(),
                               argv=underlying.argv(),
                               env=underlying.env(),
                               uris=underlying.uris(),
                               ports=underlying.ports(),
                               cpu_and_mem=underlying.cpu_and_mem(),
                               directory=underlying.directory(),
                               user=underlying.user(),
                               needs_observer=underlying.needs_observer())


class LaunchProto(object):

    """Wraps launch proto to simplify handling of format variations

    For example, the resources can be in either the task_info or the
    executor_info.
    """

    def __init__(self, proto):
        self.proto = proto

    def executor(self):
        if self.proto.HasField("task_info"):
            return None
        if self.proto.HasField("executor_info"):
            return self.proto.executor_info
        if self.proto.task_info.HasField("executor"):
            return self.proto.task_info.executor

    def command(self):
        if self.executor() is not None:
            return self.executor().command
        else:
            return self.proto.task_info.command

    def container(self):
        if self.command().HasField("container"):
            container = self.command().container
            return container.image, list(container.options)
        return "docker:///", []

    def resources(self):
        # NB: We only want the executor resources when there is no task.
        if self.proto.HasField("task_info"):
            return self.proto.task_info.resources
        else:
            return self.executor().resources

    def executor_id(self):
        if self.executor() is not None:
            return self.executor().executor_id.value
        else:
            return self.proto.task_info.task_id.value

    def container_id(self):
        return self.proto.container_id.value

    def cpu_and_mem(self):
        cpu, mem = None, None
        for r in self.resources():
            if r.name == "cpus":
                cpu = str(int(r.scalar.value * 1024))
            if r.name == "mem":
                mem = str(int(r.scalar.value)) + "m"
        return (cpu, mem)

    def env(self):
        cmd = self.command()
        self.env = [(_.name, _.value) for _ in cmd.environment.variables]
        # Add task_info.name to the environment variables
        self.env += [("TASK_INFO", self.proto.task_info.name)]
        return self.env

    def ports(self):
        resources = [_.ranges.range for _ in self.resources()
                                           if _.name == 'ports']
        ranges = [_ for __ in resources for _ in __]
        # NB: Casting long() to int() so there's no trailing 'L' in later
        #     stringifications. Ports should only ever be shorts, anyways.
        ports = [range(int(_.begin), int(_.end) + 1) for _ in ranges]
        return [port for r in ports for port in r]

    def argv(self):
        cmd = self.command()
        if cmd.HasField("value") and cmd.value != "":
            return ["sh", "-c", cmd.value]
        return []

    def uris(self):
        return list(self.command().uris)

    def needs_observer(self):
        return self.executor() is None

    def user(self):
        if self.proto.HasField("user"):
            return self.proto.user

    def directory(self):
        if self.proto.HasField("directory"):
            return self.proto.directory
