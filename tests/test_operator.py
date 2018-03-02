import json
import uuid
import random
from http_parser.http import HttpParser
from pymesos import MesosOperatorAgentDriver, MesosOperatorMasterDriver


def test_gen_request(mocker):
    mock_addr = 'mock_addr:1234'
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._master = mock_addr
    req = driver.gen_request()
    parser = HttpParser(0)
    assert len(req) == parser.execute(req, len(req))
    assert parser.is_headers_complete()
    assert parser.get_method() == 'POST'
    assert parser.get_url() == '/api/v1/operator'

    assert parser.is_partial_body()
    body = parser.recv_body()
    result = json.loads(body.decode('utf-8'))
    assert result['type'] == 'SUBSCRIBE'

    headers = {k.upper(): v for k, v in parser.get_headers().items()}
    assert headers == {
        'HOST': mock_addr,
        'CONTENT-TYPE': 'application/json',
        'ACCEPT': 'application/json',
        'CONNECTION': 'close',
        'CONTENT-LENGTH': str(len(body))
    }

    assert parser.is_message_complete()


def test_send(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    resp = mocker.Mock(
        status=200,
        read=mocker.Mock(return_value='{}')
    )
    conn = mocker.Mock(
        getresponse=mocker.Mock(return_value=resp)
    )
    driver._get_conn = mocker.Mock(return_value=conn)
    assert driver._send({}) == {}
    driver._get_conn.assert_called_once_with()
    conn.request.assert_called_once_with(
        'POST', '/api/v1/operator',
        body=json.dumps({
        }).encode('utf-8'),
        headers={
            'Content-Type': 'application/json'
        }
    )


def test_on_task_added(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._started = True
    task_info = {
        'name': 'dummy-task',
        'task_id': {'value': str(uuid.uuid4())},
        'agent_id': {'value': str(uuid.uuid4())},
        'command': {'value': 'sleep', 'arguments': ['100'], }
    }
    event = {
        'type': 'TASK_ADDED',
        'task_added': {
            'task': task_info
        }
    }
    driver.on_event(event)
    operator.taskAdded.assert_called_once_with(task_info)
    operator.taskUpdated.assert_not_called()


def test_on_task_updated(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._started = True
    task_info = {
        'task_id': {
            'value': str(uuid.uuid4())
        },
        'framework_id': {
            'value': str(uuid.uuid4())
        },
        'agent_id': {
            'value': str(uuid.uuid4())
        },
        'executor_id': {
            'value': str(uuid.uuid4())
        },
        'state': 'TASK_RUNNING'
    }
    event = {
        'type': 'TASK_UPDATED',
        'task_updated': task_info
    }
    driver.on_event(event)
    operator.taskUpdated.assert_called_once_with(task_info)
    operator.taskAdded.assert_not_called()


def test_on_framework_added(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._started = True
    framework_info = {
        'active': 'true',
        'allocated_resources': [],
        'connected': 'true',
        'framework_info': {
            'capabilities': [
                {
                    'type': 'RESERVATION_REFINEMENT'
                }
            ],
            'checkpoint': 'true',
            'failover_timeout': 0,
            'id': {
                'value': str(uuid.uuid4())
            },
            'name': 'inverse-offer-example-framework',
            'role': '*',
            'user': 'root'
        },
        'inverse_offers': [],
        'recovered': 'false',
        'registered_time': {
            'nanoseconds': 1501191957829317120
        },
        'reregistered_time': {
            'nanoseconds': 1501191957829317120
        }
    }
    event = {
        'type': 'FRAMEWORK_ADDED',
        'framework_added': {
            'framework': framework_info
        }
    }
    driver.on_event(event)
    operator.frameworkAdded.assert_called_once_with(framework_info)
    operator.frameworkUpdated.assert_not_called()


def test_on_framework_updated(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._started = True
    framework_info = {
        'active': 'true',
        'allocated_resources': [],
        'connected': 'true',
        'framework_info': {
            'capabilities': [
                {
                    'type': 'RESERVATION_REFINEMENT'
                }
            ],
            'checkpoint': 'true',
            'failover_timeout': 0,
            'id': {
                'value': str(uuid.uuid4())
            },
            'name': 'inverse-offer-example-framework',
            'role': '*',
            'user': 'root'
        },
        'inverse_offers': [],
        'recovered': 'false',
        'registered_time': {
            'nanoseconds': 1501191957829317120
        },
        'reregistered_time': {
            'nanoseconds': 1501191957829317120
        }
    }
    event = {
        'type': 'FRAMEWORK_UPDATED',
        'framework_updated': {
            'framework': framework_info
        }
    }
    driver.on_event(event)
    operator.frameworkUpdated.assert_called_once_with(framework_info)
    operator.frameworkAdded.assert_not_called()


def test_on_framework_removed(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._started = True
    framework_info = {
        'capabilities': [
            {
                'type': 'RESERVATION_REFINEMENT'
            }
        ],
        'checkpoint': 'true',
        'failover_timeout': 0,
        'id': {
            'value': str(uuid.uuid4())
        },
        'name': 'inverse-offer-example-framework',
        'role': '*',
        'user': 'root'
    }
    event = {
        'type': 'FRAMEWORK_REMOVED',
        'framework_removed': {
            'framework_info': framework_info
        }
    }
    driver.on_event(event)
    operator.frameworkRemoved.assert_called_once_with(framework_info)


def test_on_agent_added(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._started = True
    agent_info = {
        'active': 'true',
        'agent_info': {
            'hostname': '172.31.2.24',
            'id': {
                'value': str(uuid.uuid4())
            },
            'port': 5051,
            'resources': [],
        },
        'allocated_resources': [],
        'capabilities': [
            {
                'type': 'MULTI_ROLE'
            },
            {
                'type': 'HIERARCHICAL_ROLE'
            },
            {
                'type': 'RESERVATION_REFINEMENT'
            }
        ],
        'offered_resources': [],
        'pid': 'slave(1)@172.31.2.24:5051',
        'registered_time': {
            'nanoseconds': 1500993262264135000
        },
        'reregistered_time': {
            'nanoseconds': 1500993263019321000
        },
        'total_resources': [],
        'version': '1.4.0'
    }
    event = {
        'type': 'AGENT_ADDED',
        'agent_added': {
            'agent': agent_info
        }
    }
    driver.on_event(event)
    operator.agentAdded.assert_called_once_with(agent_info)


def test_on_agent_removed(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._started = True
    agent_id = str(uuid.uuid4())
    event = {
        'type': 'AGENT_REMOVED',
        'agent_removed': {
            'agent_id': {
                'value': agent_id
            }
        }
    }
    driver.on_event(event)
    operator.agentRemoved.assert_called_once_with(agent_id)


def test_get_health(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getHealth()
    driver._send.assert_called_once_with({
        'type': 'GET_HEALTH',
    })


def test_get_flags(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getFlags()
    driver._send.assert_called_once_with({
        'type': 'GET_FLAGS',
    })


def test_get_version(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getVersion()
    driver._send.assert_called_once_with({
        'type': 'GET_VERSION',
    })


def test_get_metrics(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    timeout = random.randint(0, 10) * 1000000000
    driver.getMetrics(timeout)
    driver._send.assert_called_once_with({
        'type': 'GET_METRICS',
        'get_metrics': {
            'timeout': {
                'nanoseconds': timeout
            }
        }
    })


def test_get_logging_level(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getLoggingLevel()
    driver._send.assert_called_once_with({
        'type': 'GET_LOGGING_LEVEL',
    })


def test_set_logging_level(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    duration = random.randint(0, 10) * 1000000000
    level = random.randint(0, 3)
    driver.setLoggingLevel(level, duration)
    driver._send.assert_called_once_with({
        'type': 'SET_LOGGING_LEVEL',
        'set_logging_level': {
            'duration': {
                'nanoseconds': duration
            },
            'level': level
        }
    })


def test_list_files(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    path = 'one/'
    driver.listFiles(path)
    driver._send.assert_called_once_with({
        'type': 'LIST_FILES',
        'list_files': {
            'path': path
        }
    })


def test_read_file(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    path = 'myname'
    length = random.randint(0, 10)
    offset = random.randint(0, length)
    driver.readFile(path, length, offset)
    driver._send.assert_called_once_with({
        'type': 'READ_FILE',
        'read_file': {
            'length': length,
            'offset': offset,
            'path': path
        }
    })


def test_get_state(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getState()
    driver._send.assert_called_once_with({
        'type': 'GET_STATE',
    })


def test_get_frameworks(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getFrameworks()
    driver._send.assert_called_once_with({
        'type': 'GET_FRAMEWORKS',
    })


def test_get_executors(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getExecutors()
    driver._send.assert_called_once_with({
        'type': 'GET_EXECUTORS',
    })


def test_get_tasks(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getTasks()
    driver._send.assert_called_once_with({
        'type': 'GET_TASKS',
    })


def test_get_agents(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getAgents()
    driver._send.assert_called_once_with({
        'type': 'GET_AGENTS',
    })


def test_get_roles(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getRoles()
    driver._send.assert_called_once_with({
        'type': 'GET_ROLES',
    })


def test_get_weights(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getWeights()
    driver._send.assert_called_once_with({
        'type': 'GET_WEIGHTS',
    })


def test_update_weights(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    weight_infos = [
        {
            'role': 'role',
            'weight': random.uniform(0, 10)
        }
    ]
    driver.updateWeights(weight_infos)
    driver._send.assert_called_once_with({
        'type': 'UPDATE_WEIGHTS',
        'update_weights': {
            'weight_infos': weight_infos
        }
    })


def test_get_master(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getMaster()
    driver._send.assert_called_once_with({
        'type': 'GET_MASTER',
    })


def test_reserve_resources(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    agent_id = str(uuid.uuid4())
    resources = [
        {
            'type': 'SCALAR',
            'name': 'cpus',
            'reservation': {
                'principal': 'my-principal'
            },
            'role': 'role',
            'scalar': {
                'value': random.uniform(0, 1024.0)
            }
        },
        {
            'type': 'SCALAR',
            'name': 'mem',
            'reservation': {
                'principal': 'my-principal'
            },
            'role': 'role',
            'scalar': {
                'value': random.uniform(0, 1024.0)
            }
        }
    ]
    driver.reserveResources(agent_id, resources)
    driver._send.assert_called_once_with({
        'type': 'RESERVE_RESOURCES',
        'reserve_resources': {
            'agent_id': {
                'value': agent_id
            },
            'resources': resources
        }
    })


def test_unreserve_resources(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    agent_id = str(uuid.uuid4())
    resources = [
        {
            'type': 'SCALAR',
            'name': 'cpus',
            'reservation': {
                'principal': 'my-principal'
            },
            'role': 'role',
            'scalar': {
                'value': random.uniform(0, 1024.0)
            }
        },
        {
            'type': 'SCALAR',
            'name': 'mem',
            'reservation': {
                'principal': 'my-principal'
            },
            'role': 'role',
            'scalar': {
                'value': random.uniform(0, 1024.0)
            }
        }
    ]
    driver.unreserveResources(agent_id, resources)
    driver._send.assert_called_once_with({
        'type': 'UNRESERVE_RESOURCES',
        'unreserve_resources': {
            'agent_id': {
                'value': agent_id
            },
            'resources': resources
        }
    })


def test_create_volumes(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    agent_id = str(uuid.uuid4())
    volumes = [
        {
            'type': 'SCALAR',
            'disk': {
                'persistence': {
                    'id': 'id1',
                    'principal': 'my-principal'
                },
                'volume': {
                    'container_path': 'path1',
                    'mode': 'RW'
                }
            },
            'name': 'disk',
            'role': 'role1',
            'scalar': {
                'value': random.uniform(0, 1024.0)
            }
        }
    ]
    driver.createVolumes(agent_id, volumes)
    driver._send.assert_called_once_with({
        'type': 'CREATE_VOLUMES',
        'create_volumes': {
            'agent_id': {
                'value': agent_id
            },
            'volumes': volumes
        }
    })


def test_destroy_volumes(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    agent_id = str(uuid.uuid4())
    volumes = [
        {
            'disk': {
                'persistence': {
                    'id': 'id1',
                    'principal': 'my-principal'
                },
                'volume': {
                    'container_path': 'path1',
                    'mode': 'RW'
                }
            },
            'name': 'disk',
            'role': 'role1',
            'scalar': {
                'value': random.uniform(0, 1024.0)
            },
            'type': 'SCALAR'
        }
    ]
    driver.destroyVolumes(agent_id, volumes)
    driver._send.assert_called_once_with({
        'type': 'DESTROY_VOLUMES',
        'destroy_volumes': {
            'agent_id': {
                'value': agent_id
            },
            'volumes': volumes
        }
    })


def test_get_maintenance_status(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getMaintenanceStatus()
    driver._send.assert_called_once_with({
        'type': 'GET_MAINTENANCE_STATUS',
    })


def test_get_maintenance_schedule(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getMaintenanceSchedule()
    driver._send.assert_called_once_with({
        'type': 'GET_MAINTENANCE_SCHEDULE',
    })


def test_update_maintenance_schedule(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    windows = [
        {
            'machine_ids': [
                {
                    'hostname': 'myhost'
                },
                {
                    'ip': '0.0.0.2'
                }
            ],
            'unavailability': {
                'start': {
                    'nanoseconds': 1470820233192017920
                }
            }
        }
    ]
    driver.updateMaintenanceSchedule(windows)
    driver._send.assert_called_once_with({
        'type': 'UPDATE_MAINTENANCE_SCHEDULE',
        'update_maintenance_schedule': {
            'schedule': {
                'windows': windows
            }
        }
    })


def test_start_maintenance(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    machines = [
        {
            'hostname': 'myhost',
            'ip': '0.0.0.3'
        }
    ]
    driver.startMaintenance(machines)
    driver._send.assert_called_once_with({
        'type': 'START_MAINTENANCE',
        'start_maintenance': {
            'machines': machines
        }
    })


def test_stop_maintenance(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    machines = [
        {
            'hostname': 'myhost',
            'ip': '0.0.0.3'
        }
    ]
    driver.stopMaintenance(machines)
    driver._send.assert_called_once_with({
        'type': 'STOP_MAINTENANCE',
        'stop_maintenance': {
            'machines': machines
        }
    })


def test_get_quota(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    driver.getQuota()
    driver._send.assert_called_once_with({
        'type': 'GET_QUOTA',
    })


def test_set_quota(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    quota_request = {
        'force': 'true',
        'guarantee': [
            {
                'name': 'cpus',
                'role': '*',
                'scalar': {
                    'value': random.uniform(0, 1024.0)
                },
                'type': 'SCALAR'
            },
            {
                'name': 'mem',
                'role': '*',
                'scalar': {
                    'value': random.uniform(0, 1024.0)
                },
                'type': 'SCALAR'
            }
        ],
        'role': 'role1'
    }
    driver.setQuota(quota_request)
    driver._send.assert_called_once_with({
        'type': 'SET_QUOTA',
        'set_quota': {
            'quota_request': quota_request
        }
    })


def test_remove_quota(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    role = 'role1'
    driver.removeQuota(role)
    driver._send.assert_called_once_with({
        'type': 'REMOVE_QUOTA',
        'remove_quota': {
            'role': role
        }
    })


def test_mark_agent_gone(mocker):
    operator = mocker.Mock()
    master = mocker.Mock()
    driver = MesosOperatorMasterDriver(master, operator)
    driver._send = mocker.Mock()
    agent_id = str(uuid.uuid4())
    driver.markAgentGone(agent_id)
    driver._send.assert_called_once_with({
        'type': 'MARK_AGENT_GONE',
        'mark_agent_gone': {
            'agent_id': {
                'value': agent_id
            }
        }
    })


def test_get_containers(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    driver.getContainers()
    driver._send.assert_called_once_with({
        'type': 'GET_CONTAINERS',
    })


def test_launch_nested_container(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    launch_nested_container = {
        'container_id': {
            'parent': {
                'parent': {
                    'value': str(uuid.uuid4())
                },
                'value': str(uuid.uuid4())
            },
            'value': str(uuid.uuid4())
        },
        'command': {
            'environment': {
                'variables': [
                    {
                        'name': 'ENV_VAR_KEY',
                        'type': 'VALUE',
                        'value': 'env_var_value'
                    }
                ]
            },
            'shell': 'true',
            'value': 'exit 0'
        }
    }
    driver.launchNestedContainer(launch_nested_container)
    driver._send.assert_called_once_with({
        'type': 'LAUNCH_NESTED_CONTAINER',
        'launch_nested_container': launch_nested_container,
    })


def test_wait_nested_container(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    container_id = str(uuid.uuid4())
    parent_id = str(uuid.uuid4())
    driver.waitNestedContainer(container_id, parent_id)
    driver._send.assert_called_once_with({
        'type': 'WAIT_NESTED_CONTAINER',
        'wait_nested_container': {
            'container_id': {
                'parent': {
                    'value': parent_id,
                },
                'value': container_id,
            },
        },
    })


def test_kill_nested_container(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    container_id = str(uuid.uuid4())
    parent_id = str(uuid.uuid4())
    driver.killNestedContainer(container_id, parent_id)
    driver._send.assert_called_once_with({
        'type': 'KILL_NESTED_CONTAINER',
        'kill_nested_container': {
            'container_id': {
                'parent': {
                    'value': parent_id,
                },
                'value': container_id,
            },
        },
    })


def test_launch_nested_container_session(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    launch_nested_container_session = {
        'container_id': {
            'parent': {
                'parent': {
                    'value': str(uuid.uuid4())
                },
                'value': str(uuid.uuid4())
            },
            'value': str(uuid.uuid4())
        },
        'command': {
            'environment': {
                'variables': [
                    {
                        'name': 'ENV_VAR_KEY',
                        'type': 'VALUE',
                        'value': 'env_var_value'
                    }
                ]
            },
            'shell': 'true',
            'value': 'while [ true ]; do echo $(date); sleep 1; done'
        }
    }
    driver.launchNestedContainerSession(launch_nested_container_session)
    driver._send.assert_called_once_with({
        'type': 'LAUNCH_NESTED_CONTAINER_SESSION',
        'launch_nested_container_session': launch_nested_container_session,
    }, headers={'Accept': 'application/recordio'})


def test_attach_container_input(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    container_id = 'da737efb-a9d4-4622-84ef-f55eb07b861a'
    process_ios = [
        {
            'type': 'DATA',
            'data': {
                'type': 'STDIN',
                'data': 'dGVzdAo='
            }
        },
        {
            'type': 'CONTROL',
            'control': {
                'type': 'HEARTBEAT',
                'heartbeat': {
                    'interval': {
                        'nanoseconds': 30000000000
                    }
                }
            }
        }
    ]
    driver.attachContainerInput(container_id, process_ios)
    container_id_msg = dict(
        type='ATTACH_CONTAINER_INPUT',
        attach_container_input=dict(
            type='CONTAINER_ID',
            container_id=dict(
                value='da737efb-a9d4-4622-84ef-f55eb07b861a',
            )
        ),
    )
    process_io_msg_1st = dict(
        type='ATTACH_CONTAINER_INPUT',
        attach_container_input=dict(
            type='PROCESS_IO',
            process_io=dict(type='DATA',
                            data=dict(
                                type='STDIN',
                                data='dGVzdAo=',
                            ),
                            ),
        ),
    )
    process_io_msg_2nd = dict(
        type='ATTACH_CONTAINER_INPUT',
        attach_container_input=dict(
            type='PROCESS_IO',
            process_io=dict(
                type="CONTROL",
                control=dict(
                    type="HEARTBEAT",
                    heartbeat=dict(
                        interval=dict(
                            nanoseconds=30000000000,
                        ),
                    ),
                ),
            ),
        ),
    )
    awaited_result = '153\n' + json.dumps(
        container_id_msg) + '163\n' + json.dumps(
        process_io_msg_1st) + '210\n' + json.dumps(process_io_msg_2nd)
    driver._send.assert_called_once_with(awaited_result, headers={
        'Accept': 'application/recordio'})


def test_attach_container_output(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    container_id = str(uuid.uuid4())
    driver.attachContainerOutput(container_id)
    driver._send.assert_called_once_with({
        'type': 'ATTACH_CONTAINER_OUTPUT',
        'attach_container_output': {
            'container_id': {
                'value': container_id,
            },
        },
    }, headers={'Accept': 'application/recordio',
                'Message-Accept': 'application/json'})


def test_remove_nested_container(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    container_id = str(uuid.uuid4())
    parent_id = str(uuid.uuid4())
    driver.removeNestedContainer(container_id, parent_id)
    driver._send.assert_called_once_with({
        'type': 'REMOVE_NESTED_CONTAINER',
        'remove_nested_container': {
            'container_id': {
                'parent': {
                    'value': parent_id,
                },
                'value': container_id
            },
        },
    })


def test_add_resource_provider_config(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    info = {
        'type': 'org.apache.mesos.rp.local.storage',
        'name': 'test_slrp',
        'default_reservations': [
            {
                'type': 'DYNAMIC',
                'role': 'test-role'
            }
        ],
        'storage': {
            'plugin': {
                'type': 'org.apache.mesos.csi.test',
                'name': 'test_plugin',
                'containers': [
                    {
                        'services': [
                            'CONTROLLER_SERVICE',
                            'NODE_SERVICE'
                        ],
                        'command': {
                            'shell': 'true',
                            'value': './test-csi-plugin --available_capacity=\
                            2GB --work_dir=workdir',
                            'uris': [
                                {
                                    'value': '/PATH/TO/test-csi-plugin',
                                    'executable': 'true'
                                }
                            ]
                        },
                        'resources': [
                            {'name': 'cpus', 'type': 'SCALAR',
                             'scalar': {'value': random.uniform(0, 200)}},
                            {'name': 'mem', 'type': 'SCALAR',
                             'scalar': {'value': random.uniform(0, 200)}}
                        ]
                    }
                ]
            }
        }
    }
    driver.addResourceProviderConfig(info)
    driver._send.assert_called_once_with({
        'type': 'ADD_RESOURCE_PROVIDER_CONFIG',
        'add_resource_provider_config': {
            'info': info,
        },
    })


def test_update_resource_provider_config(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    info = {
        'type': 'org.apache.mesos.rp.local.storage',
        'name': 'test_slrp',
        'default_reservations': [
            {
                'type': 'DYNAMIC',
                'role': 'test-role'
            }
        ],
        'storage': {
            'plugin': {
                'type': 'org.apache.mesos.csi.test',
                'name': 'test_plugin',
                'containers': [
                    {
                        'services': [
                            'CONTROLLER_SERVICE',
                            'NODE_SERVICE'
                        ],
                        'command': {
                            'shell': 'true',
                            'value': './test-csi-plugin --available_capacity=\
                            2GB --work_dir=workdir',
                            'uris': [
                                {
                                    'value': '/PATH/TO/test-csi-plugin',
                                    'executable': 'true'
                                }
                            ]
                        },
                        'resources': [
                            {'name': 'cpus', 'type': 'SCALAR',
                             'scalar': {'value': random.uniform(0, 200)}},
                            {'name': 'mem', 'type': 'SCALAR',
                             'scalar': {'value': random.uniform(0, 200)}}
                        ]
                    }
                ]
            }
        }
    }
    driver.updateResourceProviderConfig(info)
    driver._send.assert_called_once_with({
        'type': 'UPDATE_RESOURCE_PROVIDER_CONFIG',
        'update_resource_provider_config': {
            'info': info,
        },
    })


def test_remove_resource_provider_config(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    type = 'org.apache.mesos.rp.local.storage'
    name = 'test_slrp'
    driver.removeResourceProviderConfig(type, name)
    driver._send.assert_called_once_with({
        'type': 'REMOVE_RESOURCE_PROVIDER_CONFIG',
        'remove_resource_provider_config': {
            'type': type,
            'name': name,
        },
    })


def test_prune_images(mocker):
    agent = mocker.Mock()
    driver = MesosOperatorAgentDriver(agent)
    driver._send = mocker.Mock()
    excluded_images = [
        {'type': 'DOCKER', 'docker': {'name': 'mysql:latest'}}
    ]
    driver.pruneImages(excluded_images)
    driver._send.assert_called_once_with({
        'type': 'PRUNE_IMAGES',
        'prune_images': {
            'excluded_images': excluded_images,
        },
    })
