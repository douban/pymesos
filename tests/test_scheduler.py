import json
import uuid
import random
import string
from six.moves import range
from http_parser.http import HttpParser
from pymesos import MesosSchedulerDriver, encode_data


def test_gen_request(mocker):
    mock_addr = 'mock_addr:1234'
    sched = mocker.Mock()
    framework = {
        'failover_timeout': 0
    }
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._master = mock_addr
    req = driver.gen_request()
    parser = HttpParser(0)
    assert len(req) == parser.execute(req, len(req))
    assert parser.is_headers_complete()
    assert parser.get_method() == 'POST'
    assert parser.get_url() == '/api/v1/scheduler'

    assert parser.is_partial_body()
    body = parser.recv_body()
    result = json.loads(body.decode('utf-8'))
    assert result['type'] == 'SUBSCRIBE'
    assert result['subscribe'] == {
        'framework_info': framework
    }

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
    sched = mocker.Mock()
    framework = mocker.Mock()
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
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
        'POST', '/api/v1/scheduler',
        body=json.dumps({
        }).encode('utf-8'),
        headers={
            'Content-Type': 'application/json'
        }
    )


def test_teardown(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._send = mocker.Mock()
    driver.stream_id = str(uuid.uuid4())
    assert driver.connected
    driver._teardown()
    driver._send.assert_called_once_with({
        'type': 'TEARDOWN',
        'framework_id': {
            'value': ID
        },
    })


def test_accept_offers(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._send = mocker.Mock()
    offer_ids = [str(uuid.uuid4()) for _ in range(random.randint(1, 10))]
    operations = [{
        'type': 'LAUNCH',
        'launch': {
            'task_infos': [
                {
                    'name': '1',
                },
                {
                    'name': '2',
                }
            ]
        }
    }]
    driver.acceptOffers(offer_ids, operations)
    driver._send.assert_called_once_with({
        'type': 'ACCEPT',
        'framework_id': {
            'value': ID
        },
        'accept': {
            'offer_ids': offer_ids,
            'operations': operations,
        }
    })


def test_launch_tasks(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._send = mocker.Mock()
    offer_ids = [str(uuid.uuid4()) for _ in range(random.randint(1, 10))]
    tasks = [
        {
            'name': '1',
        },
        {
            'name': '2',
        }
    ]
    driver.launchTasks(offer_ids, tasks)
    driver._send.assert_called_once_with({
        'type': 'ACCEPT',
        'framework_id': {
            'value': ID
        },
        'accept': {
            'offer_ids': offer_ids,
            'operations': [{
                'type': 'LAUNCH',
                'launch': {
                    'task_infos': tasks,
                }
            }]
        }
    })


def test_decline_offer(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._send = mocker.Mock()
    offer_ids = [str(uuid.uuid4()) for _ in range(random.randint(1, 10))]
    driver.declineOffer(offer_ids)
    driver._send.assert_called_once_with({
        'type': 'DECLINE',
        'framework_id': {
            'value': ID
        },
        'decline': {
            'offer_ids': offer_ids
        }
    })


def test_revive_offers(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._send = mocker.Mock()
    driver._stream_id = str(uuid.uuid4())
    driver.reviveOffers()
    driver._send.assert_called_once_with({
        'type': 'REVIVE',
        'framework_id': {
            'value': ID
        },
    })


def test_acknowledge_status_update(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._send = mocker.Mock()
    agent_id = dict(value=str(uuid.uuid4()))
    task_id = dict(value=str(uuid.uuid4()))
    uid = encode_data(uuid.uuid4().bytes)
    status = {
        'agent_id': agent_id,
        'task_id': task_id,
        'uuid': uid
    }
    driver.acknowledgeStatusUpdate(status)
    driver._send.assert_called_once_with({
        'type': 'ACKNOWLEDGE',
        'framework_id': {
            'value': ID
        },
        'acknowledge': {
            'agent_id': agent_id,
            'task_id': task_id,
            'uuid': uid
        }
    })


def test_reconcile_tasks(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._send = mocker.Mock()
    task_ids = [str(uuid.uuid4()) for _ in range(random.randint(1, 10))]
    tasks = [
        {
            'task_id': {
                'value': id
            }
        }
        for id in task_ids
    ]
    driver.reconcileTasks(tasks)
    driver._send.assert_called_once_with({
        'type': 'RECONCILE',
        'framework_id': {
            'value': ID
        },
        'reconcile': {
            'tasks': tasks
        }
    })


def test_send_framework_message(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._send = mocker.Mock()
    executor_id = {'value': str(uuid.uuid4())}
    agent_id = {'value': str(uuid.uuid4())}
    message = ''.join(random.choice(string.printable)
                      for _ in range(random.randint(1, 100)))
    message = encode_data(message.encode('utf-8'))
    driver.sendFrameworkMessage(executor_id, agent_id, message)
    driver._send.assert_called_once_with({
        'type': 'MESSAGE',
        'framework_id': {
            'value': ID
        },
        'message': {
            'agent_id': agent_id,
            'executor_id': executor_id,
            'data': message,
        }
    })


def test_request_resources(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._send = mocker.Mock()
    requests = [{
        'agent_id': {'value': str(uuid.uuid4())},
        'resources': {}
    } for _ in range(random.randint(1, 10))]
    driver.requestResources(requests)
    driver._send.assert_called_once_with({
        'type': 'REQUEST',
        'framework_id': {
            'value': ID
        },
        'request': {
            'requests': requests
        }
    })


def test_on_subscribed(mocker):
    sched = mocker.Mock()
    framework = {}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver.version = '1.0.0'
    driver._started = True
    driver._master = 'mock_addr:12345'
    framework_id = {
        'value': str(uuid.uuid4())
    }

    event = {
        'type': 'SUBSCRIBED',
        'subscribed': {
            'framework_id': framework_id
        }
    }
    driver.on_event(event)
    sched.registered.assert_called_once_with(driver, framework_id, {
        'hostname': 'mock_addr',
        'port': 12345,
        'version': '1.0.0'
    })


def test_on_offers(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._started = True
    offers = [{
        'offer_id': {'value': str(uuid.uuid4())}
    } for _ in range(random.randint(1, 10))]
    event = {
        'type': 'OFFERS',
        'offers': {
            'offers': offers
        }
    }
    driver.on_event(event)
    sched.resourceOffers.assert_called_once_with(driver, offers)
    sched.inverseOffers.assert_not_called()


def test_on_offers_inverse(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._started = True
    offers = [{
        'offer_id': {'value': str(uuid.uuid4())}
    } for _ in range(random.randint(1, 10))]
    event = {
        'type': 'OFFERS',
        'offers': {
            'inverse_offers': offers
        }
    }
    driver.on_event(event)
    sched.resourceOffers.assert_not_called()
    sched.inverseOffers.assert_called_once_with(driver, offers)


def test_on_rescind(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._started = True
    offer_id = {'value': str(uuid.uuid4())}
    event = {
        'type': 'RESCIND',
        'rescind': {
            'offer_id': offer_id
        }
    }
    driver.on_event(event)
    sched.offerRescinded.assert_called_once_with(driver, offer_id)


def test_on_message(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._started = True
    executor_id = {'value': str(uuid.uuid4())}
    agent_id = {'value': str(uuid.uuid4())}
    message = ''.join(random.choice(string.printable)
                      for _ in range(random.randint(1, 100)))
    data = encode_data(message.encode('utf8'))

    event = {
        'type': 'MESSAGE',
        'message': {
            'executor_id': executor_id,
            'agent_id': agent_id,
            'data': data
        }
    }
    driver.on_event(event)
    sched.frameworkMessage.assert_called_once_with(driver, executor_id,
                                                   agent_id, data)


def test_on_failure(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._started = True
    executor_id = dict(value=str(uuid.uuid4()))
    agent_id = dict(value=str(uuid.uuid4()))
    status = random.randint(0, 256)
    event = {
        'type': 'FAILURE',
        'failure': {
            'executor_id': executor_id,
            'agent_id': agent_id,
            'status': status
        }
    }
    driver.on_event(event)
    sched.executorLost.assert_called_once_with(driver, executor_id,
                                               agent_id, status)

    event = {
        'type': 'FAILURE',
        'failure': {
            'agent_id': agent_id,
        }
    }
    driver.on_event(event)
    sched.slaveLost.assert_called_once_with(driver, agent_id)


def test_on_error(mocker):
    ID = str(uuid.uuid4())
    sched = mocker.Mock()
    framework = {'id': {'value': ID}}
    master = mocker.Mock()
    driver = MesosSchedulerDriver(sched, framework, master)
    driver._started = True
    msg = 'error message'
    event = {
        'type': 'ERROR',
        'error': {
            'message': msg
        }
    }
    driver.on_event(event)
    sched.error.assert_called_once_with(driver, msg)
