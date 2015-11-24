import os
import sys
import random
import socket
import getpass
import logging
from threading import RLock
from mesos.interface import mesos_pb2, Scheduler
from pymesos import MesosSchedulerDriver
try:
    import cPickle as pickle
except ImportError:
    import pickle

logger = logging.getLogger(__name__)
CONFIG = {}
FOREVER = 0xFFFFFFFF


class ProcScheduler(Scheduler):

    def __init__(self):
        self.framework_id = None
        self.framework = self._init_framework()
        self.executor = None
        self.master = str(CONFIG.get('master', os.environ['MESOS_MASTER']))
        self.driver = MesosSchedulerDriver(self, self.framework, self.master)
        self.procs_pending = {}
        self.procs_launched = {}
        self.slave_to_proc = {}
        self._lock = RLock()

    def _init_framework(self):
        framework = mesos_pb2.FrameworkInfo()
        framework.user = getpass.getuser()
        framework.name = repr(self)
        framework.hostname = socket.gethostname()
        return framework

    def _init_executor(self):
        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = 'default'
        executor.command.value = '%s -m %s.executor' % (
            sys.executable, __package__)
        if 'PYTHONPATH' in os.environ:
            var = executor.command.environment.variables.add()
            var.name = 'PYTHONPATH'
            var.value = os.environ['PYTHONPATH']

        executor.framework_id.value = str(self.framework_id.value)
        return executor

    def _init_task(self, proc, offer):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(proc.id)
        task.slave_id.value = offer.slave_id.value
        task.name = repr(proc)
        task.executor.MergeFrom(self.executor)
        task.data = pickle.dumps(proc.params)

        cpus = task.resources.add()
        cpus.name = 'cpus'
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = proc.cpus

        mem = task.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = proc.mem

        return task

    def _filters(self, seconds):
        f = mesos_pb2.Filters()
        f.refuse_seconds = seconds
        return f

    def registered(self, driver, framework_id, master_info):
        with self._lock:
            logger.info('Framework registered with id=%s, master=%s' % (
                framework_id, master_info))
            self.framework_id = framework_id
            self.executor = self._init_executor()

    def resourceOffers(self, driver, offers):
        def get_resources(offer):
            cpus, mem = 0.0, 0.0
            for r in offer.resources:
                if r.name == 'cpus':
                    cpus = float(r.scalar.value)
                elif r.name == 'mem':
                    mem = float(r.scalar.value)
            return cpus, mem

        with self._lock:
            random.shuffle(offers)
            for offer in offers:
                if not self.procs_pending:
                    logger.debug('Reject offers forever for no pending procs, '
                                 'offers=%s' % (offers, ))
                    driver.launchTasks(offer.id, [], self._filters(FOREVER))
                    continue

                cpus, mem = get_resources(offer)
                tasks = []
                for proc in self.procs_pending.values():
                    if cpus >= proc.cpus and mem >= proc.mem:
                        tasks.append(self._init_task(proc, offer))
                        del self.procs_pending[proc.id]
                        self.procs_launched[proc.id] = proc
                        cpus -= proc.cpus
                        mem -= proc.mem

                seconds = 5 + random.random() * 5
                driver.launchTasks(offer.id, tasks, self._filters(seconds))
                if tasks:
                    logger.info('Accept offer for procs, offer=%s, '
                                'procs=%s, filter_time=%s' % (
                                    offer,
                                    [int(t.task_id.value) for t in tasks],
                                    seconds))
                else:
                    logger.info('Retry offer for procs later, offer=%s, '
                                'filter_time=%s' % (
                                    offer, seconds))

    def statusUpdate(self, driver, update):
        with self._lock:
            proc_id = int(update.task_id.value)
            logger.info('Status update for proc, id=%s, state=%s' % (
                proc_id, update.state))
            if update.state == mesos_pb2.TASK_RUNNING:
                if update.slave_id.value in self.slave_to_proc:
                    self.slave_to_proc[update.slave_id.value].add(proc_id)
                else:
                    self.slave_to_proc[update.slave_id.value] = set([proc_id])

                proc = self.procs_launched[proc_id]
                proc._started()

            elif update.state >= mesos_pb2.TASK_FINISHED:
                proc = self.procs_launched.pop(proc_id)
                if update.slave_id.value in self.slave_to_proc:
                    self.slave_to_proc[update.slave_id.value].remove(proc_id)

                success = (update.state == mesos_pb2.TASK_FINISHED)
                message = update.message
                data = update.data and pickle.loads(update.data)
                proc._finished(success, message, data)
                driver.reviveOffers()

    def offerRescinded(self, driver, offer_id):
        with self._lock:
            if self.procs_pending:
                logger.info('Revive offers for pending procs')
                driver.reviveOffers()

    def start(self):
        self.driver.start()

    def stop(self):
        self.driver.stop()

    def submit(self, proc):
        with self._lock:
            if proc.id not in self.procs_pending:
                logger.info('Try submit proc, id=%s', (proc.id,))
                self.procs_pending[proc.id] = proc
                if len(self.procs_pending) == 1:
                    logger.info('Revive offers for pending procs')
                    self.driver.reviveOffers()
            else:
                raise ValueError('Proc with same id already submitted')
