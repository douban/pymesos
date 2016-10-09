import os
import sys
import random
import socket
import getpass
import logging
from threading import RLock
from six.moves import cPickle as pickle
from .. import Scheduler, MesosSchedulerDriver, encode_data, decode_data

logger = logging.getLogger(__name__)
CONFIG = {}
FOREVER = 0xFFFFFFFF
_TYPE_SIGNAL, = list(range(1))
MIN_CPUS = 0.01
MIN_MEMORY = 32


class ProcScheduler(Scheduler):

    def __init__(self):
        self.framework_id = None
        self.framework = self._init_framework()
        self.executor = None
        self.master = str(CONFIG.get('master', os.environ['MESOS_MASTER']))
        self.driver = MesosSchedulerDriver(self, self.framework, self.master)
        self.procs_pending = {}
        self.procs_launched = {}
        self.agent_to_proc = {}
        self._lock = RLock()

    def _init_framework(self):
        framework = dict(
            user=getpass.getuser(),
            name=repr(self),
            hostname=socket.gethostname(),
        )
        return framework

    def _init_executor(self):
        executor = dict(
            executor_id=dict(value='default'),
            framework_id=self.framework_id,
            command=dict(
                value='%s -m %s.executor' % (
                    sys.executable, __package__
                )
            ),
            resources=[
                dict(
                    name='mem',
                    type='SCALAR',
                    scalar=dict(value=MIN_MEMORY),
                ),
                dict(
                    name='cpus',
                    type='SCALAR',
                    scalar=dict(value=MIN_CPUS)
                ),
            ],
        )

        if 'PYTHONPATH' in os.environ:
            executor['command.environment'] = dict(
                variables=[
                    dict(
                        name='PYTHONPATH',
                        value=os.environ['PYTHONPATH'],
                    ),
                ]
            )

        return executor

    def _init_task(self, proc, offer):
        resources = [
            dict(
                name='cpus',
                type='SCALAR',
                scalar=dict(value=proc.cpus),
            ),
            dict(
                name='mem',
                type='SCALAR',
                scalar=dict(value=proc.mem),
            )
        ]

        if proc.gpus > 0:
            resources.append(
                dict(
                    name='gpus',
                    type='SCALAR',
                    scalar=dict(value=proc.gpus),
                )
            )

        task = dict(
            task_id=dict(value=str(proc.id)),
            name=repr(proc),
            executor=self.executor,
            agent_id=offer['agent_id'],
            data=encode_data(pickle.dumps(proc.params)),
            resources=resources,
        )

        return task

    def _filters(self, seconds):
        f = dict(refuse_seconds=seconds)
        return f

    def __repr__(self):
        return "%s[%s]: %s" % (
            self.__class__.__name__,
            os.getpid(), ' '.join(sys.argv))

    def registered(self, driver, framework_id, master_info):
        with self._lock:
            logger.info('Framework registered with id=%s, master=%s' % (
                framework_id, master_info))
            self.framework_id = framework_id
            self.executor = self._init_executor()

    def resourceOffers(self, driver, offers):
        def get_resources(offer):
            cpus, mem, gpus = 0.0, 0.0, 0
            for r in offer['resources']:
                if r['name'] == 'cpus':
                    cpus = float(r['scalar']['value'])
                elif r['name'] == 'mem':
                    mem = float(r['scalar']['value'])
                elif r['name'] == 'gpus':
                    gpus = int(r['scalar']['value'])

            return cpus, mem, gpus

        with self._lock:
            random.shuffle(offers)
            for offer in offers:
                if not self.procs_pending:
                    logger.debug('Reject offers forever for no pending procs, '
                                 'offers=%s' % (offers, ))
                    driver.declineOffer(
                        offer['id'], self._filters(FOREVER))
                    continue

                cpus, mem, gpus = get_resources(offer)
                tasks = []
                for proc in list(self.procs_pending.values()):
                    if (cpus >= proc.cpus + MIN_CPUS
                            and mem >= proc.mem + MIN_MEMORY
                            and gpus >= proc.gpus):
                        tasks.append(self._init_task(proc, offer))
                        del self.procs_pending[proc.id]
                        self.procs_launched[proc.id] = proc
                        cpus -= proc.cpus
                        mem -= proc.mem
                        gpus -= proc.gpus

                seconds = 5 + random.random() * 5
                if tasks:
                    logger.info('Accept offer for procs, offer=%s, '
                                'procs=%s, filter_time=%s' % (
                                    offer,
                                    [int(t['task_id']['value'])
                                     for t in tasks],
                                    seconds))
                    driver.launchTasks(
                        offer['id'], tasks, self._filters(seconds))
                else:
                    logger.info('Retry offer for procs later, offer=%s, '
                                'filter_time=%s' % (
                                    offer, seconds))
                    driver.declineOffer(offer['id'], self._filters(seconds))

    def _call_finished(self, proc_id, success, message, data, agent_id=None):
        with self._lock:
            proc = self.procs_launched.pop(proc_id)
            if agent_id is not None:
                if agent_id in self.agent_to_proc:
                    self.agent_to_proc[agent_id].remove(proc_id)
            else:
                for agent_id, procs in list(self.agent_to_proc.items()):
                    if proc_id in procs:
                        procs.remove(proc_id)

            proc._finished(success, message, data)

    def statusUpdate(self, driver, update):
        with self._lock:
            proc_id = int(update['task_id']['value'])
            state = update['state']
            logger.info('Status update for proc, id=%s, state=%s' % (
                proc_id, state))
            agent_id = update['agent_id']['value']
            if proc_id not in self.procs_launched:
                logger.warning(
                    'Unknown proc %s update for %s ignored',
                    proc_id, state
                )
                driver.reviveOffers()
                return

            if state == 'TASK_RUNNING':
                if agent_id in self.agent_to_proc:
                    self.agent_to_proc[agent_id].add(proc_id)
                else:
                    self.agent_to_proc[agent_id] = set([proc_id])

                proc = self.procs_launched[proc_id]
                proc._started()

            elif state not in {
                'TASK_STAGING', 'TASK_STARTING', 'TASK_RUNNING'
            }:
                success = (state == 'TASK_FINISHED')
                message = update.get('message')
                data = update.get('data')
                if data:
                    data = pickle.loads(decode_data(data))

                self._call_finished(proc_id, success, message, data, agent_id)
                driver.reviveOffers()

    def offerRescinded(self, driver, offer_id):
        with self._lock:
            if self.procs_pending:
                logger.info('Revive offers for pending procs')
                driver.reviveOffers()

    def executorLost(self, driver, executor_id, agent_id, status):
        agent_id = agent_id['value']
        with self._lock:
            for proc_id in self.agent_to_proc.pop(agent_id, []):
                self._call_finished(
                    proc_id, False, 'Executor lost', None, agent_id)

    def slaveLost(self, driver, agent_id):
        agent_id = agent_id['value']
        with self._lock:
            for proc_id in self.agent_to_proc.pop(agent_id, []):
                self._call_finished(
                    proc_id, False, 'Agent lost', None, agent_id)

    def error(self, driver, message):
        with self._lock:
            for proc in list(self.procs_pending.values()):
                self._call_finished(proc.id, False, message, None)

            for proc in list(self.procs_launched.values()):
                self._call_finished(proc.id, False, message, None)

        self.stop()

    def start(self):
        self.driver.start()

    def stop(self):
        assert not self.driver.aborted
        self.driver.stop()
        self.driver.join()

    def submit(self, proc):
        if self.driver.aborted:
            raise RuntimeError('driver already aborted')

        with self._lock:
            if proc.id not in self.procs_pending:
                logger.info('Try submit proc, id=%s', (proc.id,))
                self.procs_pending[proc.id] = proc
                if len(self.procs_pending) == 1:
                    logger.info('Revive offers for pending procs')
                    self.driver.reviveOffers()
            else:
                raise ValueError('Proc with same id already submitted')

    def cancel(self, proc):
        if self.driver.aborted:
            raise RuntimeError('driver already aborted')

        with self._lock:
            if proc.id in self.procs_pending:
                del self.procs_pending[proc.id]
            elif proc.id in self.procs_launched:
                del self.procs_launched[proc.id]
                self.driver.killTask(dict(value=str(proc.id)))

            for agent_id, procs in list(self.agent_to_proc.items()):
                procs.pop(proc.id)
                if not procs:
                    del self.agent_to_proc[agent_id]

    def send_data(self, pid, type, data):
        if self.driver.aborted:
            raise RuntimeError('driver already aborted')

        msg = encode_data(pickle.dumps((pid, type, data)))
        for agent_id, procs in list(self.agent_to_proc.items()):
            if pid in procs:
                self.driver.sendFrameworkMessage(
                    self.executor['executor_id'],
                    dict(value=agent_id),
                    msg
                )
                return

        raise RuntimeError('Cannot find agent for pid %s' % (pid,))
