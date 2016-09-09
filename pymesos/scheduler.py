import json
import logging
from six.moves.http_client import HTTPConnection
from binascii import a2b_base64
from .process import Process

logger = logging.getLogger(__name__)


class MesosSchedulerDriver(Process):

    def __init__(self, sched, framework, master_uri):
        super(MesosSchedulerDriver, self).__init__()
        self.sched = sched
        self.master_uri = master_uri
        self.framework = framework
        self.detector = None
        self.savedOffers = {}
        self._conn = None
        self.version = None

    @property
    def connected(self):
        return self.stream_id is not None

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

    def abort(self):
        self.stop()

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

        if body:
            data = json.dumps(body).encode('utf-8')
            headers['Content-Type'] = 'application/json'
        else:
            data = ''

        stream_id = self.stream_id
        if stream_id:
            headers['Mesos-Stream-Id'] = stream_id

        conn.request(method, path, body=data, headers=headers)
        resp = conn.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError('Failed to send request %s' % (data,))

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
        accept = dict(
            offer_ids=offer_ids,
            operations=[dict(
                type='LAUNCH',
                launch=dict(
                    task_infos=tasks
                ),
            )]
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
        if 'agent_id' in status:
            acknowledge['agent_id'] = status['agent_id']
        else:
            acknowledge['slave_id'] = status['slave_id']

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
        version = self.version
        assert framework_id
        assert version
        message = dict(
            executor_id=executor_id,
            data=a2b_base64(data),
        )
        version = map(int, version.split('.')[:2])
        if version >= (1, 0):
            message['agent_id'] = agent_id
        else:
            message['slave_id'] = agent_id

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
            requests=requests,
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
        request = ('POST /api/v1/scheduler  HTTP/1.1\r\nHost: %s\r\n'
                   'Content-Type: application/json\r\n'
                   'Accept: application/json\r\n'
                   'Connection: close\r\nContent-Length: %s\r\n\r\n%s') % (
            self.master, len(data), data
        )
        return request

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

    def on_offers(self, offers):
        for offer in offers:
            agent_id = offer.get('agent_id', offer['slave_id'])['value']
            self.savedOffers[offer['id']['value']] = agent_id

        self.sched.resourceOffers(self, offers)

    def on_rescind(self, offer_id):
        self.savedOffers.pop(offer_id['value'], None)
        self.sched.offerRescinded(self, offer_id)

    def on_update(self, update):
        status = update['status']
        self.sched.statusUpdate(self, status)
        self.acknowledgeStatusUpdate(status)

    def on_message(self, message):
        executor_id = message['executor_id']
        agent_id = message.get('agent_id', message['slave_id'])
        data = message['data']
        self.sched.frameworkMessage(self, executor_id, agent_id, data)

    def on_failure(self, failure):
        agent_id = failure.get('agent_id', failure['slave_id'])
        if 'executor_id' not in failure:
            self.sched.slaveLost(self, agent_id)
        else:
            self.sched.executorLost(
                self, failure['executor_id'], agent_id, failure['status']
            )

    def on_error(self, message):
        self.sched.error(self, message)

    def on_event(self, event):
        if 'type' in event:
            _type = event['type']
            if _type.lower() not in event:
                logger.error(
                    'Missing `%s` in event %s' %
                    (_type.lower(), event))
                return

            event = event[_type.lower()]
            if _type == 'SUBSCRIBED':
                self.on_subscribed(event)
            elif _type == 'OFFERS':
                self.on_offers(event)
            elif _type == 'RESCIND':
                self.on_rescind(event)
            elif _type == 'UPDATE':
                self.on_update(event)
            elif _type == 'MESSAGE':
                self.on_message(event)
            elif _type == 'FAILURE':
                self.on_failure(event)
            elif _type == 'ERROR':
                self.on_error(event)
            elif _type == 'HEARTBEAT':
                pass
            else:
                logger.error('Unknown type:%s, event:%s' % (_type, event))
        else:
            logger.error('Unknown event:%s' % (event,))
