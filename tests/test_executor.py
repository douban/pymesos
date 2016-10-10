import json
import uuid
import random
import string
from http_parser.http import HttpParser
from pymesos import MesosExecutorDriver, encode_data


def test_gen_request(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
    driver._master = agent_addr
    assert driver.framework_id == dict(value=framework_id)
    assert driver.executor_id == dict(value=executor_id)
    assert -1e-5 < driver.grace_shutdown_period < 1e-5
    assert not driver.checkpoint
    assert driver.executor is exc

    req = driver.gen_request()
    parser = HttpParser(0)
    assert len(req) == parser.execute(req, len(req))
    assert parser.is_headers_complete()
    assert parser.get_method() == 'POST'
    assert parser.get_url() == '/api/v1/executor'

    assert parser.is_partial_body()
    body = parser.recv_body()
    result = json.loads(body.decode('utf-8'))
    assert result == {
        'type': 'SUBSCRIBE',
        'framework_id': {
            'value': framework_id,
        },
        'executor_id': {
            'value': executor_id,
        },
        'subscribe': {
            'unacknowledged_tasks': [],
            'unacknowledged_updates': [],
        }
    }

    headers = {k.upper(): v for k, v in parser.get_headers().items()}
    assert headers == {
        'HOST': agent_addr,
        'CONTENT-TYPE': 'application/json',
        'ACCEPT': 'application/json',
        'CONNECTION': 'close',
        'CONTENT-LENGTH': str(len(body))
    }

    assert parser.is_message_complete()


def test_send(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
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
        'POST', '/api/v1/executor',
        body=json.dumps({}).encode('utf-8'),
        headers={
            'Content-Type': 'application/json'
        }
    )


def test_send_status_update(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
    driver._send = mocker.Mock()
    status = {}
    driver.sendStatusUpdate(status)
    driver._send.assert_called_once_with({
        'type': 'UPDATE',
        'framework_id': {
            'value': framework_id,
        },
        'executor_id': {
            'value': executor_id,
        },
        'update': {
            'status': {
                'timestamp': status['timestamp'],
                'uuid': status['uuid'],
                'source': 'SOURCE_EXECUTOR',
            }
        }
    })


def test_send_message(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
    driver._send = mocker.Mock()
    message = ''.join(random.choice(string.printable)
                      for _ in range(random.randint(1, 100)))
    message = encode_data(message.encode('utf8'))
    driver.sendFrameworkMessage(message)
    driver._send.assert_called_once_with({
        'type': 'MESSAGE',
        'framework_id': {
            'value': framework_id,
        },
        'executor_id': {
            'value': executor_id,
        },
        'message': {
            'data': message,
        }
    })


def test_on_subscribed(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
    driver._started = True
    executor_info = {
        'executor_id': {
            'value': executor_id
        },
        'framework_id': {
            'value': framework_id
        }
    }
    framework_info = {
        'id': {
            'value': framework_id
        }
    }
    event = {
        'type': 'SUBSCRIBED',
        'subscribed': {
            'executor_info': executor_info,
            'framework_info': framework_info,
            'agent_info': {}
        }
    }
    driver.on_event(event)
    exc.registered.assert_called_once_with(
        driver, executor_info, framework_info, {})


def test_on_launch(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
    driver._started = True
    task_id = str(uuid.uuid4())
    framework_info = {
        'id': {
            'value': framework_id
        }
    }
    task_info = {
        "task_id": {
            "value": task_id
        },
    }
    event = {
        'type': 'LAUNCH',
        'launch': {
            'framework_info': framework_info,
            'task': task_info
        }
    }
    driver.on_event(event)
    exc.launchTask.assert_called_once_with(driver, task_info)


def test_on_kill(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
    driver._started = True
    task_id = {
        'value': str(uuid.uuid4())
    }
    event = {
        'type': 'KILL',
        'kill': {
            'task_id': task_id
        }
    }
    driver.on_event(event)
    exc.killTask.assert_called_once_with(driver, task_id)


def test_on_acknowledged(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
    driver._started = True
    assert driver.updates == {}
    assert driver.tasks == {}
    tid = str(uuid.uuid4())
    uid = uuid.uuid4()
    driver.updates[uid] = mocker.Mock()
    driver.tasks[tid] = mocker.Mock()
    event = {
        'type': 'ACKNOWLEDGED',
        'acknowledged': {
            'task_id': {
                'value': tid
            },
            'uuid': encode_data(uid.bytes)
        }
    }
    driver.on_event(event)
    assert driver.updates == {}
    assert driver.tasks == {}


def test_on_message(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
    driver._started = True
    message = ''.join(random.choice(string.printable)
                      for _ in range(random.randint(1, 100)))
    event = {
        'type': 'MESSAGE',
        'message': {
            'data': message
        }
    }
    driver.on_event(event)
    exc.frameworkMessage.assert_called_once_with(driver, message)


def test_on_error(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
    driver._started = True
    message = ''.join(random.choice(string.printable)
                      for _ in range(random.randint(1, 100)))
    event = {
        'type': 'ERROR',
        'error': {
            'message': message
        }
    }
    driver.on_event(event)
    exc.error.assert_called_once_with(driver, message)


def test_on_shutdown(mocker):
    agent_addr = 'mock_addr:12345'
    framework_id = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())
    env = {
        'MESOS_LOCAL': 'true',
        'MESOS_AGENT_ENDPOINT': agent_addr,
        'MESOS_FRAMEWORK_ID': framework_id,
        'MESOS_EXECUTOR_ID': executor_id,
    }
    mocker.patch('os.environ', env)
    exc = mocker.Mock()
    driver = MesosExecutorDriver(exc)
    driver._started = True
    event = {
        'type': 'shutdown'
    }
    driver.on_event(event)
    exc.shutdown.assert_called_once_with(driver)
