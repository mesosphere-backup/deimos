#!/usr/bin/env python
import argparse
import os
import random
import sys
import time

import mesos
import mesos_pb2


#################################### Schedulers implement the integration tests

class Scheduler(mesos.Scheduler):
    def __init__(self):
        self.task_max = 10
        self.tasks    = []
        self.statuses = {}
    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)
    def registered(self, driver, framework_id, master):
        self.framework_id = framework_id
        print >>sys.stderr, "Registered as framework %s" % framework_id.value
    def statusUpdate(self, driver, update):
        task, code = update.task_id.value, update.state
        self.statuses[task] = code
        name = mesos_pb2.TaskState.Name(code)
        print >>sys.stderr, "Task %s is in state %s" % (task, name)
    def next_task_id(self):
        fmt = "medea-test-task%02d-framework-%s"
        return fmt % (len(self.tasks), self.framework_id.value)
    terminal = set([ mesos_pb2.TASK_FINISHED,
                     mesos_pb2.TASK_FAILED,
                     mesos_pb2.TASK_KILLED,
                     mesos_pb2.TASK_LOST ])
    failed   = set([ mesos_pb2.TASK_FAILED,
                     mesos_pb2.TASK_KILLED,
                     mesos_pb2.TASK_LOST ])

class ExecutorScheduler(Scheduler):
    def __init__(self, command, uris=[], container=None):
        Scheduler.__init__(self)
        self.command   = command
        self.uris      = uris
        self.container = container
        self.messages  = []
    def statusUpdate(self, driver, update):
        super(Scheduler, self).statusUpdate(driver, update)
        if update.state == mesos_pb2.TASK_RUNNING:
            pass                  # TODO: Send a message if we get TASK_RUNNING
        task_terminated = update.state in Scheduler.terminal
        enough_tasks    = len(self.tasks) >= self.tasks_max
        if task_terminated and enough_tasks:
            driver.stop()
    def frameworkMessage(self, driver, executor_id, slave_id, msg):
        self.messages += [msg]
        driver.killTask(update.task_id)
    def resourceOffers(self, driver, offers):
        for offer in offers:
            if len(self.tasks) >= self.tasks_max: break
            tid  = self.next_task_id()
            sid  = offer.slave_id
            task = task_with_executor(tid, sid)
            self.tasks += [task]
            driver.launchTasks(offer.id, [task])

class SleepScheduler(Scheduler):
    def __init__(self, sleep=20, uris=[], container=None):
        Scheduler.__init__(self)
        self.sleep     = sleep
        self.uris      = uris
        self.container = container
    def statusUpdate(self, driver, update):
        super(Scheduler, self).statusUpdate(driver, update)
        task_terminated = update.state in Scheduler.terminal
        enough_tasks    = len(self.tasks) >= self.tasks_max
        if task_terminated and enough_tasks:
            driver.stop()
    def resourceOffers(self, driver, offers):
        delay = int(float(self.sleep) / self.tasks_max)
        for offer in offers:
            if len(self.tasks) >= self.tasks_max: break
            time.sleep(delay)             # Space out the requests a little bit
            tid  = self.next_task_id()
            sid  = offer.slave_id
            cmd  = "sleep %d" % self.sleep
            task = task_with_command(tid, sid, cmd, self.uris, self.container)
            self.tasks += [task]
            driver.launchTasks(offer.id, [task])

class PGScheduler(Scheduler):
    def __init__(self, container="docker:///zaiste/postgresql"):
        Scheduler.__init__(self)
        self.container = container
    def statusUpdate(self, driver, update):
        super(Scheduler, self).statusUpdate(driver, update)
        if update.state == mesos_pb2.TASK_RUNNING:
            time.sleep(2)
            driver.killTask(update.task_id)       # Shutdown Postgres container
        task_terminated = update.state in Scheduler.terminal
        enough_tasks    = len(self.tasks) >= self.tasks_max
        if task_terminated and enough_tasks:
            driver.stop()
    def resourceOffers(self, driver, offers):
        for offer in offers:
            if len(self.tasks) >= self.tasks_max: break
            time.sleep(2)
            tid  = self.next_task_id()
            sid  = offer.slave_id
            task = task_with_daemon(tid, sid, self.container)
            self.tasks += [task]
            driver.launchTasks(offer.id, [task])


################################################################ Task factories

def task_with_executor(tid, sid, *args):
    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = tid
    executor.name = tid
    executor.source = "medea-test"
    executor.command.MergeFrom(command(*args))
    task = task_base(tid, sid)
    task.executor.MergeFrom(executor)
    return task

def task_with_command(tid, sid, *args):
    task = task_base(tid, sid)
    task.command.MergeFrom(command(*args))
    return task

def task_with_daemon(tid, sid, image):
    task = task_base(tid, sid)
    task.command.MergeFrom(command(image=image))
    return task

def task_base(tid, sid, cpu=0.5, ram=256):
    task_id = "medea-test-%05d" % tid
    task = mesos_pb2.TaskInfo()
    task.task_id.value = task_id
    task.slave_id.value = sid.value
    task.name = task_id
    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = cpu
    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = ram
    return task

def command(shell="", uris=[], image=None):
    command = mesos_pb2.CommandInfo()
    command.value = shell                                          # TODO: URIs
    if image:                                       # Rely on the default image
        container = mesos_pb2.CommandInfo.ContainerInfo()
        container.image = image
        command.container.MergeFrom(container)
    return command


########################################################################## Main

def cli():
    schedulers = { "sleep"    : SleepScheduler,
                   "pg"       : PGScheduler,
                   "executor" : ExecutorScheduler }
    p = argparse.ArgumentParser(prog="medea-test.py")
    p.add_argument("--master", default="localhost:5050",
                   help="Mesos master URL")
    p.add_argument("--test", choices=schedulers.keys(), default="sleep",
                   help="Test suite to use")
    p.add_argument("--test.container",
                   help="Image URL to use for test.")
    p.add_argument("--test.sleep", type=int,
                   help="Seconds to sleep (for sleep test)")
    p.add_argument("--test.command",
                   help="Command to use (for executor test)")
    parsed = p.parse_args()

    pairs = [ (k.split("test.")[1:], v) for k, v in vars(parsed).items() ]
    constructor_args = dict( (k[0], v) for k, v in pairs if len(k) == 1 and v )
    scheduler_class = schedulers[parsed.test]
    args = ", ".join( "%s=%r" % (k, v) for k, v in constructor_args.items() )
    print >>sys.stderr, "Test class: %s(%s)" % (scheduler_class.__name__, args)
    scheduler = scheduler_class(**constructor_args)

    framework = mesos_pb2.FrameworkInfo()
    framework.name = "medea-test"
    framework.user = ""
    driver = mesos.MesosSchedulerDriver(scheduler, framework, parsed.master)
    os._exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)

if __name__ == "__main__": cli()

