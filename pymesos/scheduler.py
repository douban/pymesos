import json
import logging
from binascii import b2a_base64
from six.moves.http_client import HTTPConnection
from .process import Process
from .interface import SchedulerDriver

logger = logging.getLogger(__name__)


class MesosSchedulerDriver(Process, SchedulerDriver):

    def __init__(self, sched, framework, master_uri):
        super(MesosSchedulerDriver, self).__init__()
        self.sched = sched
        self.master_uri = master_uri
        self.framework = framework
        self.detector = None
        self._conn = None
        self.version = None

    @property
    def framework_id(self):
        return self.framework.get('id')

    @framework_id.setter
    def framework_id(self, id):
        self.framework['id'] = id

    def change_master(self, master):
        super(MesosSchedulerDriver, self).change_master(master)
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self.version = None

    def start(self):
        super(MesosSchedulerDriver, self).start()
        uri = self.master_uri
        if uri.startswith('zk://') or uri.startswith('zoo://'):
            from .detector import MasterDetector
            self.detector = MasterDetector(uri[uri.index('://') + 3:], self)
            self.detector.start()
        else:
            if ':' not in uri:
                uri += ':5050'
            self.change_master(uri)

    def stop(self, failover=False):
        if not failover:
            try:
                self._teardown()
            except Exception:
                logger.exception('Failed to Teardown')

        if self.detector:
            self.detector.stop()

        super(MesosSchedulerDriver, self).stop()

    def _get_conn(self):
        if not self.connected:
            return None

        if self._conn is not None:
            return self._conn

        host, port = self.master.split(':', 2)
        port = int(port)
        self._conn = HTTPConnection(host, port, timeout=1)
        return self._conn

    def _send(self, body, path='/api/v1/scheduler', method='POST', headers={}):
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
            raise RuntimeError('Failed to send request %s: %s\n%s' % (
                resp.status, resp.read(), data))

        result = resp.read()
        if not result:
            return {}

        try:
            return json.loads(result.decode('utf-8'))
        except Exception:
            return {}

    def _teardown(self):
        framework_id = self.framework_id
        if framework_id:
            self._send(dict(
                type='TEARDOWN',
                framework_id=dict(
                    value=framework_id,
                ),
            ))

    def acceptOffers(self, offer_ids, operations, filters=None):
        if not operations:
            return self.declineOffer(offer_ids, filters=filters)

        framework_id = self.framework_id
        assert framework_id

        accept = dict(
            offer_ids=offer_ids,
            operations=operations,
        )

        if filters is not None:
            accept['filters'] = filters

        body = dict(
            type='ACCEPT',
            framework_id=dict(
                value=framework_id,
            ),
            accept=accept,
        )
        self._send(body)

    def launchTasks(self, offer_ids, tasks, filters=None):
        if not tasks:
            return self.declineOffer(offer_ids, filters=filters)

        framework_id = self.framework_id
        assert framework_id

        operations = [dict(
            type='LAUNCH',
            launch=dict(
                task_infos=tasks
            ),
        )]

        self.acceptOffers(offer_ids, operations, filters=filters)

    def declineOffer(self, offer_ids, filters=None):
        framework_id = self.framework_id
        assert framework_id
        decline = dict(
            offer_ids=offer_ids,
        )

        if filters is not None:
            decline['filters'] = filters

        body = dict(
            type='DECLINE',
            framework_id=dict(
                value=framework_id,
            ),
            decline=decline,
        )
        self._send(body)

    def reviveOffers(self):
        if not self.connected:
            return

        framework_id = self.framework_id
        assert framework_id
        body = dict(
            type='REVIVE',
            framework_id=dict(
                value=framework_id,
            ),
        )
        self._send(body)

    def killTask(self, task_id):
        framework_id = self.framework_id
        assert framework_id
        body = dict(
            type='KILL',
            framework_id=dict(
                value=framework_id,
            ),
            kill=dict(
                task_id=task_id,
            ),
        )
        self._send(body)

    def acknowledgeStatusUpdate(self, status):
        framework_id = self.framework_id
        assert framework_id
        acknowledge = dict()
        acknowledge['agent_id'] = status['agent_id']
        acknowledge['task_id'] = status['task_id']
        acknowledge['uuid'] = status['uuid']
        body = dict(
            type='ACKNOWLEDGE',
            framework_id=dict(
                value=framework_id,
            ),
            acknowledge=acknowledge,
        )
        self._send(body)

    def reconcileTasks(self, tasks):
        framework_id = self.framework_id
        assert framework_id
        body = dict(
            type='RECONCILE',
            framework_id=dict(
                value=framework_id,
            ),
            reconcile=dict(
                tasks=[dict(task_id=task['task_id']) for task in tasks],
            ),
        )
        self._send(body)

    def sendFrameworkMessage(self, executor_id, agent_id, data):
        framework_id = self.framework_id
        assert framework_id
        message = dict(
            agent_id=agent_id,
            executor_id=executor_id,
            data=b2a_base64(data.encode('utf-8')).rstrip(),
        )

        body = dict(
            type='MESSAGE',
            framework_id=dict(
                value=framework_id,
            ),
            message=message,
        )
        self._send(body)

    def requestResources(self, requests):
        framework_id = self.framework_id
        assert framework_id
        body = dict(
            type='REQUEST',
            framework_id=dict(
                value=framework_id,
            ),
            request=dict(
                requests=requests,
            ),
        )
        self._send(body)

    def onNewMasterDetectedMessage(self, data):
        master = None
        try:
            parsed = json.loads(data)
            if parsed and "address" in parsed:
                ip = parsed["address"].get("ip")
                port = parsed["address"].get("port")
                if ip and port:
                    master = "%s:%s" % (ip, port)
        except Exception:
            logger.exception("No JSON content, probably connecting "
                             "to older Mesos version.")

        if master:
            self.change_master(master)

    def onNoMasterDetectedMessage(self):
        self.change_master(None)

    def gen_request(self):
        data = json.dumps(dict(
            type='SUBSCRIBE',
            subscribe=dict(
                framework_info=self.framework
            ),
        ))
        request = ('POST /api/v1/scheduler HTTP/1.1\r\nHost: %s\r\n'
                   'Content-Type: application/json\r\n'
                   'Accept: application/json\r\n'
                   'Connection: close\r\nContent-Length: %s\r\n\r\n%s') % (
                       self.master, len(data), data
        )
        return request.encode('utf-8')

    def on_close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self.version = None

        self.sched.disconnected(self)

    def on_subscribed(self, info):
        self.version = self._send('', path='/version', method='GET')['version']
        reregistered = (self.framework_id is not None)
        self.framework_id = info['framework_id']['value']
        hostname, port = self.master.split(':', 2)
        port = int(port)
        master_info = dict(
            hostname=hostname,
            port=port,
            version=self.version
        )
        if reregistered:
            self.sched.reregistered(self, master_info)
        else:
            framework_id = dict(
                value=self.framework_id
            )
            self.sched.registered(self, framework_id, master_info)

    def on_offers(self, event):
        offers = event['offers']
        self.sched.resourceOffers(self, offers)

    def on_rescind(self, event):
        offer_id = event['offer_id']
        self.sched.offerRescinded(self, offer_id)

    def on_update(self, event):
        status = event['status']
        self.sched.statusUpdate(self, status)
        self.acknowledgeStatusUpdate(status)

    def on_message(self, message):
        executor_id = message['executor_id']
        agent_id = message['agent_id']
        data = message['data']
        self.sched.frameworkMessage(self, executor_id, agent_id, data)

    def on_failure(self, failure):
        agent_id = failure['agent_id']
        if 'executor_id' not in failure:
            self.sched.slaveLost(self, agent_id)
        else:
            self.sched.executorLost(
                self, failure['executor_id'], agent_id, failure['status']
            )

    def on_error(self, event):
        message = event['message']
        self.sched.error(self, message)

    def on_event(self, event):
        if 'type' in event:
            _type = event['type'].lower()
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
