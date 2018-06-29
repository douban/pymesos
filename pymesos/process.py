import os
import re
import sys
import json
import time
import math
import errno
import random
import select
import signal
import socket
import logging
from six import reraise
from six.moves import _thread as thread
from six.moves.http_client import OK, TEMPORARY_REDIRECT, SERVICE_UNAVAILABLE
from six.moves.urllib.parse import urlparse
from threading import Thread, RLock
from http_parser.http import HttpParser
from .utils import DAY


def _strerror(err):
    try:
        return os.strerror(err)
    except (ValueError, OverflowError, NameError):
        if err in errno.errorcode:
            return errno.errorcode[err]
        return "Unknown error %s" % err


def _handle_sigint(signum, frame):
    global _prev_handler, _exc_info
    assert signum == signal.SIGINT
    if _exc_info is not None:
        exc_info = _exc_info
        _exc_info = None
        reraise(*exc_info)
    elif _prev_handler is not None:
        return _prev_handler(signum, frame)

    raise KeyboardInterrupt


_exc_info = None
_prev_handler = signal.signal(signal.SIGINT, _handle_sigint)
LENGTH_PATTERN = re.compile(br'\d+\n')
logger = logging.getLogger(__name__)
PIPE_BUF = getattr(select, 'PIPE_BUF', 4096)

SLEEP_TIMEOUT_SCALE = 2
SLEEP_TIMEOUT_INIT = 2
SLEEP_TIMEOUT_MAX = 300
SELECT_TIMEOUT = 2


class Connection(object):

    def __init__(self, addr, callback):
        host, port = addr.split(':', 2)
        port = int(port)
        self._addr = (host, port)
        self._sock = socket.socket()
        self._sock.setblocking(0)
        self.connected = False
        try:
            self._sock.connect(self._addr)
        except socket.error as e:
            if e.errno != errno.EAGAIN and e.errno != errno.EINPROGRESS:
                raise
        self._parser = HttpParser()
        self._callback = callback
        self._stream_id = None
        self._request = callback.gen_request()
        self._response = b''

    @property
    def addr(self):
        return self._addr

    @property
    def stream_id(self):
        return self._stream_id

    def handle_connect_event(self):
        err = self._sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err == 0:
            self.connected = True
            return ""
        return _strerror(err)

    def write(self):
        try:
            sent = self._sock.send(self._request)
            self._request = self._request[sent:]
            return True
        except socket.error as e:
            if e.errno == errno.EAGAIN:
                return True

            logger.exception('Failed to send to %s', self._addr)
            return False

    def read(self):
        try:
            buf = self._sock.recv(PIPE_BUF)
            n_recv = len(buf)
            if n_recv == 0:
                logger.error('Remote %s closed', self.addr)
                return False

            n_parsed = self._parser.execute(buf, n_recv)
            if n_parsed != n_recv:
                if hasattr(self._parser, 'errno'):
                    # using http_parser.pyparser

                    from http_parser.pyparser import INVALID_CHUNK
                    if self._parser.errno == INVALID_CHUNK:
                        # need more chunk data
                        return True

                    if self._parser.errno:
                        raise RuntimeError(
                            'Failed to parse, code:%s %s' % (
                                self._parser.errno,
                                self._parser.errstr,
                            )
                        )

                else:
                    raise RuntimeError(
                        'Failed to parse, code:%s' % (
                            self._parser.get_errno(),
                        )
                    )

            if self._stream_id is None and self._parser.is_headers_complete():
                code = self._parser.get_status_code()
                if code == TEMPORARY_REDIRECT:
                    headers = {
                        k.upper(): v
                        for k, v in list(self._parser.get_headers().items())
                    }
                    new_master = headers['LOCATION']
                    new_master = urlparse(new_master).netloc or new_master
                    logger.warning(
                        'Try to redirect to new master: %s', new_master
                    )
                    self._callback.change_master(new_master)
                    return False

                elif code == SERVICE_UNAVAILABLE:
                    logger.warning('Master is not available, retry.')
                    return False

                elif code != OK:
                    msg = self._parser.recv_body()
                    if not self._parser.is_message_complete():
                        msg += ' ...'

                    raise RuntimeError('Failed with HTTP %s: %s' % (code, msg))
                if not self._parser.is_chunked():
                    raise RuntimeError('Response is not chunked')

                headers = {
                    k.upper(): v
                    for k, v in list(self._parser.get_headers().items())
                }
                self._stream_id = headers.get('MESOS-STREAM-ID', '')
                self._callback.stream_id = self._stream_id

            if self._parser.is_partial_body():
                self._response += self._parser.recv_body()
                while True:
                    m = LENGTH_PATTERN.match(self._response)
                    if not m:
                        break

                    captured = m.group(0)
                    length = int(captured.strip())
                    if len(self._response) < len(captured) + length:
                        break

                    data = self._response[
                        len(captured):len(captured) + length]
                    self._response = self._response[
                        len(captured) + length:]
                    try:
                        event = json.loads(data.decode('utf-8'))
                    except Exception:
                        logger.exception('Failed parse json %s', data)
                        raise

                    try:
                        self._callback.process_event(event)
                    except Exception:
                        logger.exception('Failed to process event')
                        raise

            if self._parser.is_message_complete():
                logger.debug('Event stream ended')
                return False

            return True
        except socket.error as e:
            if e.errno == errno.EAGAIN:
                return True

            logger.exception('Failed to recv from %s', self._addr)
            return False

    def want_write(self):
        return bool(self._request)

    def fileno(self):
        return self._sock.fileno()

    def close(self):
        self._sock.close()
        self._sock = None
        self.connected = False
        self._parser = None
        self._request = None
        self._response = None
        self._callback.on_close()


class Process(object):

    def __init__(self, master=None, timeout=DAY):
        self._master = None
        self._started = False
        self._lock = RLock()
        self._wakeup_fds = None
        self._io_thread = None
        self._new_master = master
        self._stream_id = None
        self._timeout = timeout

    @property
    def aborted(self):
        with self._lock:
            return not self._started

    @property
    def master(self):
        with self._lock:
            return self._master

    @property
    def stream_id(self):
        with self._lock:
            return self._stream_id

    @stream_id.setter
    def stream_id(self, _stream_id):
        with self._lock:
            self._stream_id = _stream_id

    @property
    def connected(self):
        return self.stream_id is not None

    def gen_request(self):
        raise NotImplementedError

    def on_event(self, event):
        raise NotImplementedError

    def on_close(self):
        raise NotImplementedError

    def process_event(self, event):
        if self._started:
            self.on_event(event)

    def change_master(self, new_master):
        with self._lock:
            self._new_master = new_master

        self._notify()

    def _notify(self):
        with self._lock:
            if self._wakeup_fds:
                os.write(self._wakeup_fds[1], b'\0')

    def _shutdown(self):
        pass

    def _run(self):
        try:
            with self._lock:
                _wakeup_fd = self._wakeup_fds[0]

            conn = None
            self.stream_id = None
            num_conn_retry = 0
            connect_deadline = 0

            while True:
                if not conn and self._master:
                    conn = Connection(self._master, self)

                    # reset deadline at first retry of every period
                    if num_conn_retry < 1:
                        connect_deadline = self._timeout + time.time()

                    if time.time() > connect_deadline:
                        raise Exception("connect reach deadline")

                to_write = set()
                to_read = set([_wakeup_fd])
                if conn:
                    to_read.add(conn.fileno())
                    if conn.want_write():
                        to_write.add(conn.fileno())

                readable, writeable, _ = select.select(to_read, to_write, [],
                                                       SELECT_TIMEOUT)

                if _wakeup_fd in readable:
                    with self._lock:
                        if not self._started:
                            break

                        if self._new_master != self._master:
                            if conn is not None:
                                to_read.discard(conn.fileno())
                                to_write.discard(conn.fileno())
                                conn.close()
                                conn = None
                                self.stream_id = None
                            self._master = self._new_master
                            num_conn_retry = 0

                    readable.remove(_wakeup_fd)
                    os.read(_wakeup_fd, PIPE_BUF)

                if not conn:
                    continue

                if not conn.connected:
                    err = conn.handle_connect_event()
                    if err:
                        sleep_timeout = self._backoff(num_conn_retry)
                        num_conn_retry += 1
                        conn.close()
                        conn = None

                        deadline = time.strftime('%Y-%m-%d %H:%M:%S',
                                                 time.localtime(
                                                     connect_deadline))
                        logger.warning(
                            'connect to %s error: %s,'
                            ' sleep %ds and try again,'
                            ' deadline: %s',
                            self._master, err, sleep_timeout, deadline)

                        time.sleep(sleep_timeout)
                        continue
                    else:
                        num_conn_retry = 0

                if writeable:
                    if not conn.write():
                        conn.close()
                        conn = None
                        self.stream_id = None
                        continue

                if readable:
                    if not conn.read():
                        conn.close()
                        conn = None
                        self.stream_id = None
        except Exception:
            logger.exception('Thread abort:')
            with self._lock:
                self._started = False

            global _exc_info
            _exc_info = sys.exc_info()
            thread.interrupt_main()

        finally:
            self._shutdown()

            if conn:
                conn.close()
                conn = None

            with self._lock:
                r, w = self._wakeup_fds
                os.close(r)
                os.close(w)
                self._wakeup_fds = None

    def _backoff(self, num_conn_retry):
        def _random_time(new, old):
            max_shift = (new - old) / SLEEP_TIMEOUT_SCALE
            return random.uniform(-max_shift, max_shift)

        new_timeout = SLEEP_TIMEOUT_INIT * math.pow(SLEEP_TIMEOUT_SCALE,
                                                    num_conn_retry)
        old_timeout = new_timeout / SLEEP_TIMEOUT_SCALE

        return min(SLEEP_TIMEOUT_MAX,
                   new_timeout + _random_time(new_timeout, old_timeout))

    def start(self):
        with self._lock:
            if self._started:
                logger.warning('Process already started!')
                return

        if self._io_thread:
            self.join()

        self._wakeup_fds = os.pipe()
        self._started = True
        self._io_thread = Thread(target=self._run, name='Process IO')
        self._io_thread.daemon = True
        self._io_thread.start()

    def abort(self):
        self.stop()

    def stop(self):
        with self._lock:
            self._started = False

        self._notify()

    def join(self):
        if self._io_thread:
            self._io_thread.join()
            self._io_thread = None

    def run(self):
        self.start()
        self.join()
