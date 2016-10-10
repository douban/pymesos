import os
import sys
import time
import atexit
import select
import socket
import signal
import logging
from six import string_types
from threading import RLock, Thread, Condition
from subprocess import PIPE, STDOUT
from .scheduler import ProcScheduler, CONFIG, _TYPE_SIGNAL

logger = logging.getLogger(__name__)


class Redirector(object):

    def __init__(self):
        rfd, wfd = os.pipe()
        self._listeners = {}
        self._readers = {}
        self._writers = {}
        self._proc_fds = {}
        self._proc_callback = {}
        self._lock = RLock()
        self._wakeup_fd = wfd
        self._aborted = False
        self._io_thread = Thread(
            target=Redirector._loop, args=(self, rfd),
            name='Redirector'
        )
        self._io_thread.daemon = True
        self._io_thread.start()

    def _clear(self, fd):
        r = self._listeners.pop(fd, None) \
            or self._readers.pop(fd, None) \
            or self._writers.pop(fd, None)
        if not r:
            fd.close()
            return

        f, pid = r[:2]
        self._proc_fds[pid].remove(fd)
        if not self._proc_fds[pid]:
            del self._proc_fds[pid]
            callback = self._proc_callback.pop(pid, None)
            if callback:
                callback()

        fd.close()
        f.close()

    def _loop(self, rfd):
        MINIMAL_INTERVAL = 0.1
        BUFFER_SIZE = select.PIPE_BUF
        while True:
            more_to_read = False
            with self._lock:
                if self._aborted:
                    break
                to_read = [rfd] + list(self._listeners.keys())
                to_read += list(self._writers.keys()) + \
                    list(self._readers.keys())
                to_write = list(self._readers.keys())

            readable, writeable, _ = select.select(to_read, to_write, [])
            with self._lock:
                for fd in readable:
                    if fd in self._listeners:
                        _fd, _ = fd.accept()
                        logger.info('[%s]Accept conn:%s' % (fd, _fd))
                        f, pid, readonly = self._listeners.pop(fd)
                        self._proc_fds[pid].remove(fd)
                        self._proc_fds[pid].add(_fd)
                        fd.close()
                        if readonly:
                            self._readers[_fd] = (f, pid)
                        else:
                            self._writers[_fd] = (f, pid)

                    elif fd in self._writers:
                        f, pid = self._writers[fd]
                        ret = select.select([], [f], [], 0)
                        if not ret[1]:
                            continue

                        buf = fd.recv(BUFFER_SIZE)
                        if buf:
                            f.write(buf)
                            more_to_read = bool(
                                select.select([fd], [], [], 0)[0])
                        else:
                            logger.info('Closing writer %s', fd)
                            self._clear(fd)

                    elif fd in self._readers:
                        logger.info('Closing reader %s', fd)
                        self._clear(fd)

                    elif fd == rfd:
                        os.read(rfd, BUFFER_SIZE)

                    else:
                        fd.close()

                for fd in writeable:
                    if fd in self._readers:
                        f, pid = self._readers[fd]
                        ret = select.select([f], [], [], 0)
                        if not ret[0]:
                            continue

                        buf = f.read(BUFFER_SIZE)
                        if buf:
                            try:
                                fd.sendall(buf)
                                more_to_read = bool(
                                    select.select([f], [], [])[0])
                            except socket.error:
                                logger.info('Closing reader %s', fd)
                                self._clear(fd)
                        else:
                            logger.info('Closing reader %s', fd)
                            self._clear(fd)

                    else:
                        fd.close()

            if not more_to_read:
                with self._lock:
                    to_read = [rfd] + list(self._listeners.keys())

                select.select(to_read, [], [], MINIMAL_INTERVAL)

        for pid in list(self._proc_fds.keys()):
            for fd in list(self._proc_fds.get(pid, [])):
                self._clear(fd)

        os.close(rfd)

    def _wakeup(self):
        os.write(self._wakeup_fd, '\0')

    def _register(self, pid, f, readonly=False):
        lfd = socket.socket()
        lfd.bind(('0.0.0.0', 0))
        _, port = lfd.getsockname()
        lfd.listen(1)
        logger.info('Listen %s for %s' % (port, f))
        with self._lock:
            self._listeners[lfd] = (f, pid, readonly)
            self._proc_fds[pid].add(lfd)

        self._wakeup()

        return port

    def unregister(self, pid):
        with self._lock:
            logger.info('Unregister %s', pid)
            for fd in list(self._proc_fds.get(pid, [])):
                self._clear(fd)

        self._wakeup()

    def register(self, pid, stdin, stdout, stderr, callback=None):
        with self._lock:
            self._proc_fds[pid] = set()
            self._proc_callback[pid] = callback

        return (self._register(pid, stdin, readonly=True),
                self._register(pid, stdout),
                self._register(pid, stderr))

    def stop(self):
        with self._lock:
            self._aborted = True
        os.close(self._wakeup_fd)
        self._io_thread.join()


_STARTING, _RUNNING, _STOPPED = list(range(3))
_STARTING_TIMEOUT = 5 * 60
UNKNOWN_ERROR = -255


class Popen(object):
    _next_id = 0
    _scheduler = None
    _redirector = None

    def __init__(self, args, bufsize=0, executable=None,
                 stdin=None, stdout=None, stderr=None,
                 preexec_fn=None, close_fds=None, shell=False,
                 cwd=None, env=None, universal_newlines=False,
                 startupinfo=None, creationflags=0,
                 cpus=None, mem=None, gpus=None):

        kw = dict(list(locals().items()))
        a = (args,)

        kw.pop('self')
        kw.pop('args')
        close_fds = kw.pop('close_fds')
        if close_fds is False:
            logger.warning('Can not support `close_fds=False`, '
                           'no fds will be inherrited.')

        stdin = kw.pop('stdin', None)
        stdout = kw.pop('stdout', None)
        stderr = kw.pop('stderr', None)
        cpus = kw.pop('cpus', None)
        mem = kw.pop('mem', None)
        gpus = kw.pop('gpus', None)

        kw['cwd'] = kw.get('cwd') or os.getcwd()
        kw['env'] = kw.get('env') or dict(list(os.environ.items()))

        self.id = self._new_id()
        self.cpus = float(cpus or CONFIG.get('default_cpus', 1.0))
        self.mem = float(mem or CONFIG.get('default_mem', 1024.0))
        self.gpus = int(gpus or CONFIG.get('default_gpus', 0))
        self.pid = None
        self.returncode = None
        self._returncode = None
        self._a = a
        self._kw = kw
        self._exc = None
        self._state = _STARTING
        self._io_waiting = True
        self._cond = Condition()

        self._prepare_handlers(stdin, stdout, stderr)
        self._submit()
        with self._cond:
            deadline = time.time() + _STARTING_TIMEOUT
            while True:
                if self._state != _STARTING:
                    break

                delta = deadline - time.time()
                if deadline <= 0:
                    raise RuntimeError('Too long to start!')

                self._cond.wait(delta)

        if self._exc:
            raise self._exc

    @classmethod
    def _new_id(cls):
        cls._next_id += 1
        return cls._next_id

    def _submit(self):
        cls = self.__class__
        if not cls._scheduler:
            cls._scheduler = ProcScheduler()
            cls._scheduler.start()
            atexit.register(cls._scheduler.stop)

        cls._scheduler.submit(self)

    def _prepare_handlers(self, stdin, stdout, stderr):
        def _dup_file(f, *a, **kw):
            fd = f if isinstance(f, int) else f.fileno()
            return os.fdopen(os.dup(fd), *a, **kw)

        cls = self.__class__
        if not cls._redirector:
            cls._redirector = Redirector()
            atexit.register(cls._redirector.stop)

        if stdin == PIPE:
            r, w = os.pipe()
            _in = os.fdopen(r, 'rb', 0)
            self.stdin = os.fdopen(w, 'wb', 0)
        else:
            self.stdin = None
            if stdin is None:
                _in = _dup_file(sys.stdin, 'rb', 0)
            else:
                _in = _dup_file(stdin, 'rb', 0)

        if stdout == PIPE:
            r, w = os.pipe()
            _out = os.fdopen(w, 'wb', 0)
            self.stdout = os.fdopen(r, 'rb', 0)
        else:
            self.stdout = None
            if stdout is None:
                _out = _dup_file(sys.stdout, 'wb', 0)
            else:
                _out = _dup_file(stdout, 'wb', 0)

        if stderr == PIPE:
            r, w = os.pipe()
            _err = os.fdopen(w, 'wb', 0)
            self.stderr = os.fdopen(r, 'rb', 0)
        else:
            self.stderr = None
            if stderr is None:
                _err = _dup_file(sys.stderr, 'wb', 0)
            elif stderr == STDOUT:
                _err = _dup_file(_out, 'wb', 0)
            else:
                _err = _dup_file(sys.stderr, 'wb', 0)

        self._handlers = cls._redirector.register(
            self.id, _in, _out, _err, self._io_complete)

    def _io_complete(self):
        with self._cond:
            self._io_waiting = False
            self._cond.notify()

    def _kill(self):
        cls = self.__class__
        cls._redirector.unregister(self.id)

    def __repr__(self):
        args = self._a[0]
        if isinstance(args, string_types):
            cmd = args
        else:
            cmd = ' '.join(args)

        return '%s: %s' % (self.__class__.__name__, cmd)

    @property
    def params(self):
        return dict(
            a=self._a,
            kw=self._kw,
            cpus=self.cpus,
            mem=self.mem,
            gpus=self.gpus,
            handlers=self._handlers,
            hostname=socket.gethostname(),
        )

    def _started(self):
        logger.info('Started')
        with self._cond:
            self._state = _RUNNING
            self._cond.notify()

    def _finished(self, success, message, data):
        logger.info('Sucess:%s message:%s', success, message)
        if success:
            self._returncode, self._exc = data
        else:
            self._returncode, self._exc = data or (
                UNKNOWN_ERROR, None)
            self._kill()

        with self._cond:
            self._state = _STOPPED
            self._cond.notify()

    def poll(self):
        with self._cond:
            if self._state == _STOPPED and not self._io_waiting:
                if self.stdin and not self.stdin.closed:
                    self.stdin.close()
                    self.stdin = None

                if self.stdout and not self.stdout.closed:
                    self.stdout.close()
                    self.stdout = None

                if self.stderr and not self.stderr.closed:
                    self.stderr.close()
                    self.stderr = None

                self.returncode = self._returncode

            return self.returncode

    def wait(self):
        with self._cond:
            while self.poll() is None:
                self._cond.wait()

        return self.returncode

    def communicate(self, input=None):
        BUFFER_SIZE = select.PIPE_BUF
        buf = input and input[:]
        out = None
        err = None
        to_write = [_f for _f in [self.stdin] if _f]
        to_read = [_f for _f in [self.stdout, self.stderr] if _f]

        while True:
            can_wait = True
            readable, writeable, _ = select.select(to_read, to_write, [], 0)
            if writeable:
                if buf:
                    can_wait = False
                    size = os.write(self.stdin.fileno(), buf[:BUFFER_SIZE])
                    buf = buf[size:]
                else:
                    if self.stdin and not self.stdin.closed:
                        self.stdin.close()

                    to_write = []

            if readable:
                can_wait = False
                if self.stdout in readable:
                    if out is None:
                        out = self.stdout.read(BUFFER_SIZE)
                    else:
                        out += self.stdout.read(BUFFER_SIZE)

                if self.stderr in readable:
                    if err is None:
                        err = self.stderr.read(BUFFER_SIZE)
                    else:
                        err += self.stderr.read(BUFFER_SIZE)

            with self._cond:
                if self.poll() is None:
                    if can_wait:
                        self._cond.wait(0.1)

                else:
                    return (out, err)

    def send_signal(self, signal):
        cls = self.__class__
        cls._scheduler.send_data(self.id, _TYPE_SIGNAL, signal)

    def terminate(self):
        self.send_signal(signal.SIGTERM)

    def kill(self):
        self.send_signal(signal.SIGKILL)

    def cancel(self):
        cls = self.__class__
        cls._scheduler.cancel(self)
        self._kill()
        self.returncode = UNKNOWN_ERROR
