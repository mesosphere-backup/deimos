#!/usr/bin/env python
import argparse
import collections
import os
import logging
import random
import signal
import sys
import threading
import time

import google.protobuf as pb

os.environ["GLOG_minloglevel"] = "3"        # Set before mesos module is loaded
import mesos
import mesos_pb2


#################################### Schedulers implement the integration tests

class Scheduler(mesos.Scheduler):
    def __init__(self, trials=10):
        self.token    = "%08x" % random.getrandbits(32)
        self.trials   = trials
        self.tasks    = []
        self.statuses = {}
        self.log      = log.getChild("scheduler")
        self.loggers  = {}
    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)
    def registered(self, driver, framework_id, master):
        self.framework_id = framework_id
        self.log.info("Registered with ID:\n  %s" % framework_id.value)
    def statusUpdate(self, driver, update):
        task, code = update.task_id.value, update.state
        if self.statuses.get(task, None) in Scheduler.terminal:
            self.loggers[task].info(present_status(update) + " (redundant)")
        else:
            self.loggers[task].info(present_status(update))
            self.statuses[task] = code
    def all_tasks_done(self):
        agg = [_ for _ in self.statuses.values() if _ in Scheduler.terminal]
        return len(agg) >= self.trials
    def sum_up(self):
        sums = [ "%s=%d" % (k, v) for k, v in self.task_status_summary() ]
        log.info(" ".join(sums))
    def task_status_summary(self):
        counts = collections.defaultdict(int)
        for task, code in self.statuses.items():
            counts[code] += 1
        return [ (mesos_pb2.TaskState.Name(code), count)
                 for code, count in counts.items() ]
    def next_task_id(self):
        short_id = "%s.task-%02d" % (self.token, len(self.tasks))
        long_id  = "deimos-test." + short_id
        self.loggers[long_id] = log.getChild(short_id)
        return long_id
    terminal = set([ mesos_pb2.TASK_FINISHED,
                     mesos_pb2.TASK_FAILED,
                     mesos_pb2.TASK_KILLED,
                     mesos_pb2.TASK_LOST ])
    failed   = set([ mesos_pb2.TASK_FAILED,
                     mesos_pb2.TASK_KILLED,
                     mesos_pb2.TASK_LOST ])

class SleepScheduler(Scheduler):
    wiki = "https://en.wikipedia.org/wiki/Main_Page"
    def __init__(self, sleep=10, uris=[wiki], container=None, trials=5):
        Scheduler.__init__(self, trials)
        self.sleep     = sleep
        self.uris      = uris
        self.container = container
        self.done      = []
    def statusUpdate(self, driver, update):
        super(type(self), self).statusUpdate(driver, update)
        if self.all_tasks_done():
            self.sum_up()
            driver.stop()
    def resourceOffers(self, driver, offers):
        delay = int(float(self.sleep) / self.trials)
        for offer in offers:
            if len(self.tasks) >= self.trials: break
          # time.sleep(self.sleep + 0.5)
            time.sleep(delay)                    # Space out the requests a bit
            tid  = self.next_task_id()
            sid  = offer.slave_id
            cmd  = "date -u +%T ; sleep " + str(self.sleep) + " ; date -u +%T"
            task = task_with_command(tid, sid, cmd, self.uris, self.container)
            self.tasks += [task]
            self.loggers[tid].info(present_task(task))
            driver.launchTasks(offer.id, [task])

class PGScheduler(Scheduler):
    def __init__(self, sleep=10,
                       container="docker:///zaiste/postgresql",
                       trials=10):
        Scheduler.__init__(self, trials)
        self.container = container
        self.sleep = sleep
    def statusUpdate(self, driver, update):
        super(type(self), self).statusUpdate(driver, update)
        if update.state == mesos_pb2.TASK_RUNNING:
            def end_task():
                time.sleep(self.sleep)
                driver.killTask(update.task_id)
            thread = threading.Thread(target=end_task)
            thread.daemon = True
            thread.start()
        if self.all_tasks_done():
            self.sum_up()
            driver.stop()
    def resourceOffers(self, driver, offers):
        for offer in offers:
            if len(self.tasks) >= self.trials: break
            tid  = self.next_task_id()
            sid  = offer.slave_id
            task = task_with_daemon(tid, sid, self.container)
            self.tasks += [task]
            self.loggers[tid].info(present_task(task))
            driver.launchTasks(offer.id, [task])

class ExecutorScheduler(Scheduler):
    sh = "python deimos-test.py --executor"
    this = "file://" + os.path.abspath(__file__)
    libmesos = "docker:///libmesos/ubuntu"
    shutdown_message = "shutdown"
    def __init__(self, command=sh, uris=[this], container=libmesos, trials=10):
        Scheduler.__init__(self, trials)
        self.command   = command
        self.uris      = uris
        self.container = container
        self.messages  = []
        self.executor  = "deimos-test.%s.executor" % self.token
    def statusUpdate(self, driver, update):
        super(type(self), self).statusUpdate(driver, update)
        if self.all_tasks_done():
            sid = update.slave_id
            eid = mesos_pb2.ExecutorID()
            eid.value = self.executor
            driver.sendFrameworkMessage(eid, sid, type(self).shutdown_message)
            self.sum_up()
            driver.stop()
    def frameworkMessage(self, driver, eid, sid, msg):
        self.messages += [msg]
        driver.killTask(update.task_id)
    def resourceOffers(self, driver, offers):
        for offer in offers:
            if len(self.tasks) >= self.trials: break
            tid  = self.next_task_id()
            task = task_with_executor(tid, offer.slave_id, self.executor,
                                      self.command, self.uris, self.container)
            self.tasks += [task]
            self.loggers[tid].info(present_task(task))
            driver.launchTasks(offer.id, [task])

class ExecutorSchedulerExecutor(mesos.Executor):
    def launchTask(self, driver, task):
        def run():
            log.info("Running task %s" % task.task_id.value)
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)
            log.info("Sent: TASK_RUNNING")
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            update.data = "ping"
            driver.sendStatusUpdate(update)
            log.info("Sent: TASK_FINISHED")
        thread = threading.Thread(target=run)
        thread.daemon = True
        thread.start()
    def frameworkMessage(self, driver, message):
        if message == ExecutorScheduler.shutdown_message:
            log.warning("Received shutdown message: %s", message)
            driver.stop()
        else:
            log.warning("Unexpected message: %s", message)


################################################################ Task factories

def task_with_executor(tid, sid, eid, *args):
    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = eid
    executor.name = eid
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
    task = mesos_pb2.TaskInfo()
    task.task_id.value = tid
    task.slave_id.value = sid.value
    task.name = tid
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
    command.value = shell
    for uri in uris:
        command.uris.add().value = uri
    if image:                      # Rely on the default image when none is set
        container = mesos_pb2.CommandInfo.ContainerInfo()
        container.image = image
        command.container.MergeFrom(container)
    return command

def present_task(task):
    if task.HasField("executor"):
        token, body = "executor", task.executor
    else:
        token, body = "command", task.command
    lines = pb.text_format.MessageToString(body).strip().split("\n")
    return "\n  %s {\n    %s\n  }" % (token, "\n    ".join(lines))

def present_status(update):
    info = mesos_pb2.TaskState.Name(update.state)
    if update.state in Scheduler.failed and update.HasField("message"):
        info += '\n  message: "%s"' % update.message
    return info


########################################################################## Main

def cli():
    schedulers = { "sleep"    : SleepScheduler,
                   "pg"       : PGScheduler,
                   "executor" : ExecutorScheduler }
    p = argparse.ArgumentParser(prog="deimos-test.py")
    p.add_argument("--master", default="localhost:5050",
                   help="Mesos master URL")
    p.add_argument("--test", choices=schedulers.keys(), default="sleep",
                   help="Test scheduler to use")
    p.add_argument("--executor", action="store_true", default=False,
                   help="Runs the executor instead of a test scheduler")
    p.add_argument("--test.container",
                   help="Image URL to use (for any test)")
    p.add_argument("--test.uris", action="append",
                   help="Pass any number of times to add URIs (for any test)")
    p.add_argument("--test.trials", type=int,
                   help="Number of tasks to run (for any test)")
    p.add_argument("--test.sleep", type=int,
                   help="Seconds to sleep (for sleep test)")
    p.add_argument("--test.command",
                   help="Command to use (for executor test)")
    parsed = p.parse_args()

    if parsed.executor:
        log.info("Mesos executor mode was chosen")
        driver = mesos.MesosExecutorDriver(ExecutorSchedulerExecutor())
        code = driver.run()
        log.info(mesos_pb2.Status.Name(code))
        driver.stop()
        if code != mesos_pb2.DRIVER_STOPPED:
            log.error("Driver died in an anomalous state")
            os._exit(2)
        os._exit(0)

    pairs = [ (k.split("test.")[1:], v) for k, v in vars(parsed).items() ]
    constructor_args = dict( (k[0], v) for k, v in pairs if len(k) == 1 and v )
    scheduler_class = schedulers[parsed.test]
    scheduler = scheduler_class(**constructor_args)
    args = ", ".join( "%s=%r" % (k, v) for k, v in constructor_args.items() )
    log.info("Testing: %s(%s)" % (scheduler_class.__name__, args))

    framework = mesos_pb2.FrameworkInfo()
    framework.name = "deimos-test"
    framework.user = ""
    driver = mesos.MesosSchedulerDriver(scheduler, framework, parsed.master)
    code = driver.run()
    log.info(mesos_pb2.Status.Name(code))
    driver.stop()
    ################  2 => driver problem  1 => tests failed  0 => tests passed
    if code != mesos_pb2.DRIVER_STOPPED:
        log.error("Driver died in an anomalous state")
        log.info("Aborted: %s(%s)" % (scheduler_class.__name__, args))
        os._exit(2)
    if any(_ in Scheduler.failed for _ in scheduler.statuses.values()):
        log.error("Test run failed -- not all tasks made it")
        log.info("Failure: %s(%s)" % (scheduler_class.__name__, args))
        os._exit(1)
    log.info("Success: %s(%s)" % (scheduler_class.__name__, args))
    os._exit(0)

logging.basicConfig(format="%(asctime)s.%(msecs)03d %(name)s %(message)s",
                    datefmt="%H:%M:%S", level=logging.DEBUG)
log = logging.getLogger("deimos-test")

if __name__ == "__main__":
    def handler(signum, _):
        log.warning("Signal: " + str(signum))
        os._exit(-signum)
    signal.signal(signal.SIGINT, handler)
    cli()

