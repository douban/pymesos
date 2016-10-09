import os
import sys
import time
import errno
import socket
import logging
import traceback
import subprocess
from threading import Condition
from six.moves import cPickle as pickle
from .. import Executor, MesosExecutorDriver, encode_data, decode_data
from .scheduler import _TYPE_SIGNAL

logger = logging.getLogger(__name__)


class ProcExecutor(Executor):

    def __init__(self):
        self.procs = {}
        self.pid_to_proc = {}
        self.cond = Condition()

    def registered(self, driver, executor_info, framework_info, agent_info):
        self.agent_id = agent_info['id']

    def reregistered(self, driver, agent_info):
        self.agent_id = agent_info['id']

    def abort(self):
        Executor.abort(self)
        self.cond.notify()

    def reply_status(self, driver, proc_id, state, message='', data=tuple()):
        update = dict(
            task_id=dict(value=str(proc_id)),
            agent_id=self.agent_id,
            timestamp=time.time(),
            state=state,
        )

        if message:
            update['message'] = message

        if data:
            update['data'] = encode_data(pickle.dumps(data))

        driver.sendStatusUpdate(update)

    def launchTask(self, driver, task):
        logger.info('Launch task')
        proc_id = int(task['task_id']['value'])
        self.reply_status(driver, proc_id, 'TASK_RUNNING')
        params = pickle.loads(decode_data(task['data']))
        a = params['a']
        kw = params['kw']
        mem = params['mem']
        handlers = params['handlers']
        hostname = params['hostname']

        for i, key in enumerate(['stdin', 'stdout', 'stderr']):
            kw[key] = s = socket.socket()
            logger.info('Connect %s:%s for %s' % (hostname, handlers[i], key))
            s.connect((hostname, handlers[i]))

        preexec_fn = kw.pop('preexec_fn', None)
        kw.pop('close_fds', None)

        def _preexec():
            import resource
            (soft, hard) = resource.getrlimit(resource.RLIMIT_AS)
            assert mem > 0, 'Task memory %s should be positive' % (mem,)
            _mem = mem * 1024 * 1024
            limit = _mem if soft < 0 or _mem < soft else soft
            resource.setrlimit(resource.RLIMIT_AS, (limit, hard))

            if preexec_fn is not None:
                preexec_fn()

        try:
            p = subprocess.Popen(*a, preexec_fn=_preexec, close_fds=True, **kw)
        except:
            exc_type, exc_value, tb = sys.exc_info()
            # Save the traceback and attach it to the exception object
            exc_lines = traceback.format_exception(exc_type,
                                                   exc_value,
                                                   tb)
            exc_value.child_traceback = ''.join(exc_lines)
            self.reply_status(driver, proc_id, 'TASK_FAILED',
                              data=(None, exc_value))
            logger.exception('Exec failed')
            return
        finally:
            kw['stdin'].close()
            kw['stdout'].close()
            kw['stderr'].close()

        with self.cond:
            self.procs[proc_id] = p
            self.pid_to_proc[p.pid] = proc_id
            self.cond.notify()

    def killTask(self, driver, task_id):
        logger.info('Kill task')
        with self.cond:
            proc_id = int(task_id['value'])
            if proc_id in self.procs:
                self.procs[proc_id].kill()

    def shutdown(self, driver):
        logger.info('Executor shutdown')
        with self.cond:
            for proc in list(self.procs.values()):
                proc.kill()

    def disconnected(self, driver):
        with self.cond:
            if driver.aborted:
                self.cond.notify()

    def run(self, driver):
        driver.start()
        while not driver.aborted:
            try:
                logger.debug('start waiting childrean...')
                pid, state = os.waitpid(-1, 0)
                logger.debug('stop waiting childrean...')

                with self.cond:
                    if pid in self.pid_to_proc:
                        proc_id = self.pid_to_proc.pop(pid)
                        proc = self.procs.pop(proc_id)
                        h = state >> 8
                        l = state & 0x7F
                        returncode = -l or h
                        success = not l
                        logger.info('Proc[%s:%s] terminated. success=%s, '
                                    'returncode=%s', proc_id, pid, success,
                                    returncode)
                        if success:
                            self.reply_status(driver, proc_id,
                                              'TASK_FINISHED',
                                              data=(returncode, None))
                        else:
                            self.reply_status(driver, proc_id,
                                              'TASK_KILLED',
                                              data=(returncode, None))

            except OSError as e:
                if e.errno != errno.ECHILD:
                    raise

                with self.cond:
                    while not driver.aborted and not self.procs:
                        logger.debug('start waiting procs...')
                        self.cond.wait()
                        logger.debug('stop waiting procs...')

        with self.cond:
            for proc in list(self.procs.values()):
                proc.kill()
                self.reply_status(driver, proc_id,
                                  'TASK_KILLED')
            self.pid_to_proc.clear()
            self.procs.clear()

        driver.join()

    def frameworkMessage(self, driver, msg):
        pid, type, data = pickle.loads(decode_data(msg))
        logger.info('Recv framework message pid:%s, type:%s, data:%s',
                    pid, type, data)

        with self.cond:
            if pid not in self.procs:
                logger.error('Cannot find pid:%s to send message', pid)
                return

            p = self.procs[pid]
            if type == _TYPE_SIGNAL:
                sig = int(data)
                p.send_signal(sig)


if __name__ == '__main__':
    log_format = '%(asctime)-15s [%(levelname)s] [%(name)-9s] %(message)s'
    logging.basicConfig(format=log_format, level=logging.DEBUG)
    executor = ProcExecutor()
    driver = MesosExecutorDriver(executor)
    executor.run(driver)
