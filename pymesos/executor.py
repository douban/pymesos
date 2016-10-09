import os
import json
import time
import uuid
import signal
import logging
from threading import Thread
from addict import Dict
from six.moves.http_client import HTTPConnection
from .process import Process
from .interface import ExecutorDriver
from .utils import parse_duration, encode_data, decode_data

logger = logging.getLogger(__name__)


class MesosExecutorDriver(Process, ExecutorDriver):
    _timeout = 10

    def __init__(self, executor, use_addict=False):
        env = os.environ
        agent_endpoint = env['MESOS_AGENT_ENDPOINT']
        super(MesosExecutorDriver, self).__init__(master=agent_endpoint)

        framework_id = env['MESOS_FRAMEWORK_ID']
        assert framework_id
        self.framework_id = dict(value=framework_id)
        executor_id = env['MESOS_EXECUTOR_ID']
        self.executor_id = dict(value=executor_id)
        grace_shutdown_period = env.get('MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD')
        if grace_shutdown_period:
            self.grace_shutdown_period = parse_duration(grace_shutdown_period)
        else:
            self.grace_shutdown_period = 0.0

        self.checkpoint = bool(env.get('MESOS_CHECKPOINT'))
        self.local = bool(env.get('MESOS_LOCAL'))

        self.executor = executor
        self.framework_info = None
        self.executor_info = None
        self.tasks = {}
        self.updates = {}
        self._conn = None
        self._dict_cls = Dict if use_addict else dict

    def _delay_kill(self):
        def _():
            try:
                time.sleep(self.grace_shutdown_period)
                os.killpg(0, signal.SIGKILL)
            except Exception:
                logger.exception('Failed to force kill executor')

        t = Thread(target=_)
        t.daemon = True
        t.start()

    def gen_request(self):
        body = json.dumps(dict(
            type='SUBSCRIBE',
            framework_id=self.framework_id,
            executor_id=self.executor_id,
            subscribe=dict(
                unacknowledged_tasks=list(self.tasks.values()),
                unacknowledged_updates=list(self.updates.values()),
            ),
        ))

        request = ('POST /api/v1/executor HTTP/1.1\r\nHost: %s\r\n'
                   'Content-Type: application/json\r\n'
                   'Accept: application/json\r\n'
                   'Connection: close\r\nContent-Length: %s\r\n\r\n%s') % (
                       self.master, len(body), body
        )
        return request.encode('utf-8')

    def on_close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self.version = None

        self.executor.disconnected(self)
        if not self.checkpoint:
            if not self.local:
                self._delay_kill()
            self.executor.shutdown(self)
            self.abort()

    def on_event(self, event):
        if 'type' in event:
            _type = event['type'].lower()
            if _type == 'shutdown':
                self.on_shutdown()
                return

            if _type == 'heartbeat':
                return

            if _type not in event:
                logger.error(
                    'Missing `%s` in event %s' %
                    (_type, event))
                return

            event = event[_type]
            func_name = 'on_%s' % (_type,)
            func = getattr(self, func_name, None)
            if func is not None:
                func(event)
            else:
                logger.error('Unknown type:%s, event:%s' % (_type, event))
        else:
            logger.error('Unknown event:%s' % (event,))

    def on_subscribed(self, info):
        executor_info = info['executor_info']
        framework_info = info['framework_info']
        agent_info = info['agent_info']
        assert executor_info['executor_id'] == self.executor_id
        assert framework_info['id'] == self.framework_id

        if self.executor_info is None or self.framework_info is None:
            self.executor_info = executor_info
            self.framework_info = framework_info
            self.executor.registered(
                self, self._dict_cls(executor_info),
                self._dict_cls(framework_info), self._dict_cls(agent_info)
            )
        else:
            self.executor.reregistered(self, self._dict_cls(agent_info))

    def on_launch(self, event):
        task_info = event['task']
        task_id = task_info['task_id']['value']
        assert task_id not in self.tasks
        self.tasks[task_id] = task_info
        self.executor.launchTask(self, self._dict_cls(task_info))

    def on_kill(self, event):
        task_id = event['task_id']
        self.executor.killTask(self, self._dict_cls(task_id))

    def on_acknowledged(self, event):
        task_id = event['task_id']['value']
        uuid_ = uuid.UUID(bytes=decode_data(event['uuid']))
        self.updates.pop(uuid_, None)
        self.tasks.pop(task_id, None)

    def on_message(self, event):
        data = event['data']
        self.executor.frameworkMessage(self, data)

    def on_error(self, event):
        message = event['message']
        self.executor.error(self, message)

    def on_shutdown(self):
        if not self.local:
            self._delay_kill()
        self.executor.shutdown(self)
        self.abort()

    def _get_conn(self):
        if not self.connected:
            return None

        if self._conn is not None:
            return self._conn

        host, port = self.master.split(':', 2)
        port = int(port)
        self._conn = HTTPConnection(host, port, timeout=self._timeout)
        return self._conn

    def _send(self, body, path='/api/v1/executor', method='POST', headers={}):
        with self._lock:
            conn = self._get_conn()
            if conn is None:
                raise RuntimeError('Not connected yet')

            if body != '':
                data = json.dumps(body).encode('utf-8')
                headers['Content-Type'] = 'application/json'
            else:
                data = ''

            stream_id = self.stream_id
            if stream_id:
                headers['Mesos-Stream-Id'] = stream_id

            try:
                conn.request(method, path, body=data, headers=headers)
                resp = conn.getresponse()
            except Exception:
                self._conn.close()
                self._conn = None
                raise

            if resp.status < 200 or resp.status >= 300:
                raise RuntimeError(
                    'Failed to send request code=%s, message=%s' % (
                        resp.status, resp.read()
                    )
                )

            result = resp.read()
            if not result:
                return {}

            try:
                return json.loads(result.decode('utf-8'))
            except Exception:
                return {}

    def sendStatusUpdate(self, status):
        if 'timestamp' not in status:
            status['timestamp'] = int(time.time())

        if 'uuid' not in status:
            status['uuid'] = encode_data(uuid.uuid4().bytes)

        if 'source' not in status:
            status['source'] = 'SOURCE_EXECUTOR'

        body = dict(
            type='UPDATE',
            executor_id=self.executor_id,
            framework_id=self.framework_id,
            update=dict(
                status=status,
            ),
        )
        self._send(body)

    def sendFrameworkMessage(self, data):
        body = dict(
            type='MESSAGE',
            executor_id=self.executor_id,
            framework_id=self.framework_id,
            message=dict(
                data=data,
            ),
        )
        self._send(body)
