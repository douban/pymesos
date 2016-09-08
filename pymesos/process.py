import os
import logging
from threading import Thread, Rlock

logger = logging.getLogger(__name__)

class Process(object):
    def __init__(self):
        self._lock = Rlock()
        self._wakeup_fds = os.pipe()

    def _run(self):
        _wakeup_fd = self._wakeup_fds[0]

    def start():
        pass

    def stop():
        pass

