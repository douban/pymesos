import json
import logging
from threading import RLock

from addict import Dict
from six.moves.http_client import HTTPConnection

from .interface import OperatorDaemonDriver
from .process import Process
from .utils import DAY

logger = logging.getLogger(__name__)


class MesosOperatorDaemonDriver(OperatorDaemonDriver):
    _timeout = 10

    def __init__(self, daemon_uri):
        self.init(daemon_uri)

    def init(self, daemon_uri):
        """
        :param daemon_uri: masterHost:5050 or agentHost:5051
        """
        self._daemon = daemon_uri
        self._conn = None

    def _get_conn(self):
        if self._conn is not None:
            return self._conn

        host, port = self._daemon.split(':', 2)
        port = int(port)
        self._conn = HTTPConnection(host, port, timeout=self._timeout)
        return self._conn

    def _send(self, body, path='/api/v1/operator', method='POST', headers={}):
        with self._lock:
            conn = self._get_conn()
            if conn is None:
                raise RuntimeError('Not connected yet')

            if body != '':
                data = json.dumps(body).encode('utf-8')
                headers['Content-Type'] = 'application/json'
            else:
                data = ''

            # stream_id = self.stream_id
            # if stream_id:
            #     headers['Mesos-Stream-Id'] = stream_id

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

    def getHealth(self):
        body = dict(
            type='GET_HEALTH',
        )
        return self._send(body)

    def getFlags(self):
        body = dict(
            type='GET_FLAGS',
        )
        return self._send(body)

    def getVersion(self):
        body = dict(
            type='GET_VERSION',
        )
        return self._send(body)

    def getMetrics(self, timeout):
        body = dict(
            type='GET_METRICS',
            get_metrics=dict(
                timeout=dict(
                    nanoseconds=timeout,
                )
            )
        )
        return self._send(body)

    def getLoggingLevel(self):
        body = dict(
            type='GET_LOGGING_LEVEL',
        )
        return self._send(body)

    def setLoggingLevel(self, level, duration):
        body = dict(
            type='SET_LOGGING_LEVEL',
            set_logging_level=dict(
                duration=dict(
                    nanoseconds=duration,
                ),
                level=level,
            )
        )
        return self._send(body)

    def listFiles(self, path):
        body = dict(
            type='LIST_FILES',
            list_files=dict(
                path=path,
            ),
        )
        return self._send(body)

    def readFile(self, path, length, offset):
        body = dict(
            type='READ_FILE',
            read_file=dict(
                length=length,
                offset=offset,
                path=path,
            )
        )
        return self._send(body)

    def getState(self):
        body = dict(
            type='GET_STATE',
        )
        return self._send(body)

    def getFrameworks(self):
        body = dict(
            type='GET_FRAMEWORKS',
        )
        return self._send(body)

    def getExecutors(self):
        body = dict(
            type='GET_EXECUTORS',
        )
        return self._send(body)

    def getTasks(self):
        body = dict(
            type='GET_TASKS',
        )
        return self._send(body)


class MesosOperatorMasterDriver(Process, MesosOperatorDaemonDriver):
    def __init__(self, master_uri, operator=None, use_addict=False,
                 timeout=DAY):
        """

        :param master_uri:
        :param operator: Optional. Only if you want to send requests that
        result in a stream of events (SUBSCRIBE).
        :type operator: OperatorMaster
        """
        super(MesosOperatorMasterDriver, self).__init__(master=master_uri,
                                                        timeout=timeout)
        super(MesosOperatorMasterDriver, self).init(master_uri)
        self.operator = operator
        self.master_uri = master_uri
        self._dict_cls = Dict if use_addict else dict

    def start(self):
        super(MesosOperatorMasterDriver, self).start()
        uri = self.master_uri
        if uri.startswith('zk://') or uri.startswith('zoo://'):
            from .detector import MasterDetector
            self.detector = MasterDetector(uri[uri.index('://') + 3:], self)
            self.detector.start()
        else:
            if ':' not in uri:
                uri += ':5050'
            self.change_master(uri)

    def gen_request(self):
        body = json.dumps(
            dict(
                type='SUBSCRIBE',
            ),
        )

        request = ('POST /api/v1/operator HTTP/1.1\r\nHost: %s\r\n'
                   'Content-Type: application/json\r\n'
                   'Accept: application/json\r\n'
                   'Connection: close\r\nContent-Length: %s\r\n\r\n%s') % (
                      self.master, len(body), body
                  )
        return request.encode('utf-8')

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

    def on_close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def on_subscribed(self, info):
        state = info['get_state']
        logger.info(
            'Operator client subscribed with cluster state: %s' % state)

    def on_task_added(self, event):
        task_info = event['task']
        self.operator.taskAdded(self._dict_cls(task_info))

    def on_task_updated(self, event):
        self.operator.taskUpdated(self._dict_cls(event))

    def on_framework_added(self, event):
        framework_info = event['framework']
        self.operator.frameworkAdded(self._dict_cls(framework_info))

    def on_framework_updated(self, event):
        framework_info = event['framework']
        self.operator.frameworkUpdated(self._dict_cls(framework_info))

    def on_framework_removed(self, event):
        framework_info = event['framework_info']
        self.operator.frameworkRemoved(self._dict_cls(framework_info))

    def on_agent_added(self, event):
        agent_info = event['agent']
        self.operator.agentAdded(self._dict_cls(agent_info))

    def on_agent_removed(self, event):
        agent_id = event['agent_id']['value']
        self.operator.agentRemoved(agent_id)

    def getAgents(self):
        body = dict(
            type='GET_AGENTS',
        )
        return self._send(body)

    def getRoles(self):
        body = dict(
            type='GET_ROLES',
        )
        return self._send(body)

    def getWeights(self):
        body = dict(
            type='GET_WEIGHTS',
        )
        return self._send(body)

    def updateWeights(self, weight_infos):
        body = dict(
            type='UPDATE_WEIGHTS',
            update_weights=dict(
                weight_infos=[dict(role=weight_info['role'],
                                   weight=weight_info['weight']) for
                              weight_info in
                              weight_infos],
            ),
        )
        self._send(body)

    def getMaster(self):
        body = dict(
            type='GET_MASTER',
        )
        return self._send(body)

    def reserveResources(self, agent_id, resources):
        body = dict(
            type='RESERVE_RESOURCES',
            reserve_resources=dict(
                agent_id=dict(value=agent_id),
                resources=resources,
            ),
        )
        self._send(body)

    def unreserveResources(self, agent_id, resources):
        body = dict(
            type='UNRESERVE_RESOURCES',
            unreserve_resources=dict(
                agent_id=dict(value=agent_id),
                resources=resources,
            ),
        )
        self._send(body)

    def createVolumes(self, agent_id, volumes):
        body = dict(
            type='CREATE_VOLUMES',
            create_volumes=dict(
                agent_id=dict(value=agent_id),
                volumes=volumes,
            ),
        )
        self._send(body)

    def destroyVolumes(self, agent_id, volumes):
        body = dict(
            type='DESTROY_VOLUMES',
            destroy_volumes=dict(
                agent_id=dict(value=agent_id),
                volumes=volumes,
            ),
        )
        self._send(body)

    def getMaintenanceStatus(self):
        body = dict(
            type='GET_MAINTENANCE_STATUS',
        )
        return self._send(body)

    def getMaintenanceSchedule(self):
        body = dict(
            type='GET_MAINTENANCE_SCHEDULE',
        )
        return self._send(body)

    def updateMaintenanceSchedule(self, windows):
        body = dict(
            type='UPDATE_MAINTENANCE_SCHEDULE',
            update_maintenance_schedule=dict(
                schedule=dict(
                    windows=windows
                ),
            ),
        )
        self._send(body)

    def startMaintenance(self, machines):
        body = dict(
            type='START_MAINTENANCE',
            start_maintenance=dict(
                machines=[dict(
                    hostname=machine['hostname'], ip=machine['ip']) for machine
                    in machines],
            ),
        )
        self._send(body)

    def stopMaintenance(self, machines):
        body = dict(
            type='STOP_MAINTENANCE',
            stop_maintenance=dict(
                machines=[dict(
                    hostname=machine['hostname'], ip=machine['ip']) for machine
                    in machines],
            ),
        )
        self._send(body)

    def getQuota(self):
        body = dict(
            type='GET_QUOTA',
        )
        return self._send(body)

    def setQuota(self, quota_request):
        body = dict(
            type='SET_QUOTA',
            set_quota=dict(
                quota_request=quota_request,
            ),
        )
        self._send(body)

    def removeQuota(self, role):
        body = dict(
            type='REMOVE_QUOTA',
            remove_quota=dict(
                role=role,
            ),
        )
        self._send(body)

    def markAgentGone(self, agent_id):
        body = dict(
            type='MARK_AGENT_GONE',
            mark_agent_gone=dict(
                agent_id=dict(
                    value=agent_id,
                ),
            ),
        )
        self._send(body)


class MesosOperatorAgentDriver(MesosOperatorDaemonDriver):
    def __init__(self, agent_uri):
        super(MesosOperatorAgentDriver, self).__init__(agent_uri)
        self._lock = RLock()

    def getContainers(self, show_nested=False, show_standalone=False):
        body = dict(
            type='GET_CONTAINERS',
            get_containers=dict(
                show_nested=show_nested,
                show_standalone=show_standalone,
            ),
        )
        return self._send(body)

    def launchNestedContainer(self, launch_nested_container):
        body = dict(
            type='LAUNCH_NESTED_CONTAINER',
            launch_nested_container=launch_nested_container,
        )
        self._send(body)

    def waitNestedContainer(self, container_id, parent_id=None):
        body = dict(
            type='WAIT_NESTED_CONTAINER',
            wait_nested_container=dict(
                container_id=dict(
                    value=container_id,
                ),
            ),
        )
        if parent_id is not None:
            body['wait_nested_container']['container_id']['parent'] = dict(
                value=parent_id, )
        self._send(body)

    def killNestedContainer(self, container_id, parent_id=None):
        body = dict(
            type='KILL_NESTED_CONTAINER',
            kill_nested_container=dict(
                container_id=dict(
                    value=container_id,
                ),
            ),
        )
        if parent_id is not None:
            body['kill_nested_container']['container_id']['parent'] = dict(
                value=parent_id, )
        self._send(body)

    def launchNestedContainerSession(self, launch_nested_container_session):
        body = dict(
            type='LAUNCH_NESTED_CONTAINER_SESSION',
            launch_nested_container_session=launch_nested_container_session,
        )
        headers = {'Accept': 'application/recordio'}
        self._send(body, headers=headers)

    def attachContainerInput(self, container_id, process_ios):
        # The first message sent over an ATTACH_CONTAINER_INPUT stream must be
        # of type CONTAINER_ID and contain the ContainerID of the container
        # being attached to.
        msg = dict(
            type='ATTACH_CONTAINER_INPUT',
            attach_container_input=dict(
                type='CONTAINER_ID',
                container_id=dict(
                    value=container_id,
                )
            ),
        )
        # Messages are encoded in RecordIO format
        sort_keys = True
        msg_str = json.dumps(msg, sort_keys=sort_keys)
        record_io = '{}\n{}'.format(len(msg_str), msg_str)
        body = ''
        body += record_io
        # Subsequent messages must be of type PROCESS_IO
        # Template for PROCESS_IO messages
        process_io = dict(
            type='ATTACH_CONTAINER_INPUT',
            attach_container_input=dict(
                type='PROCESS_IO',
                process_io='DUMMY',
            )
        )
        for msg in process_ios:
            if msg['type'] != 'DATA' and msg['type'] != 'CONTROL':
                raise ValueError(
                    'PROCESS_IO messages may contain subtypes of either DATA \
                    or CONTROL')
            process_io['attach_container_input']['process_io'] = msg
            msg_str = json.dumps(process_io, sort_keys=sort_keys)
            record_io = '{}\n{}'.format(len(msg_str), msg_str)
            body += record_io
        headers = {'Accept': 'application/recordio'}
        self._send(body, headers=headers)

    def attachContainerOutput(self, container_id):
        body = dict(
            type='ATTACH_CONTAINER_OUTPUT',
            attach_container_output=dict(
                container_id=dict(
                    value=container_id,
                )
            ),
        )
        headers = {'Accept': 'application/recordio',
                   'Message-Accept': 'application/json'}
        return self._send(body, headers=headers)

    def removeNestedContainer(self, container_id, parent_id=None):
        body = dict(
            type='REMOVE_NESTED_CONTAINER',
            remove_nested_container=dict(
                container_id=dict(
                    value=container_id,
                ),
            ),
        )
        if parent_id is not None:
            body['remove_nested_container']['container_id']['parent'] = dict(
                value=parent_id, )
        self._send(body)

    def addResourceProviderConfig(self, info):
        body = dict(
            type='ADD_RESOURCE_PROVIDER_CONFIG',
            add_resource_provider_config=dict(
                info=info,
            ),
        )
        self._send(body)

    def updateResourceProviderConfig(self, info):
        body = dict(
            type='UPDATE_RESOURCE_PROVIDER_CONFIG',
            update_resource_provider_config=dict(
                info=info,
            ),
        )
        self._send(body)

    def removeResourceProviderConfig(self, type, name):
        body = dict(
            type='REMOVE_RESOURCE_PROVIDER_CONFIG',
            remove_resource_provider_config=dict(
                type=type,
                name=name,
            ),
        )
        self._send(body)

    def pruneImages(self, excluded_images=None):
        body = dict(
            type='PRUNE_IMAGES',
        )
        if excluded_images is not None:
            body['prune_images'] = dict(
                excluded_images=excluded_images,
            )
        self._send(body)
