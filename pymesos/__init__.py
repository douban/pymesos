import sys
import logging

from .interface import Scheduler, Executor, OperatorMaster
from .scheduler import MesosSchedulerDriver
from .executor import MesosExecutorDriver
from .operator_v1 import MesosOperatorMasterDriver, MesosOperatorAgentDriver
from .utils import encode_data, decode_data


LOG_FMT = '%(asctime)-15s [%(levelname)s] [%(threadName)s]' \
      ' [%(name)-9s:%(lineno)d] %(message)s'
DATE_FMT = '%Y-%m-%d %H:%M:%S'

logger = logging.getLogger('pymesos')
handler = logging.StreamHandler(stream=sys.stderr)
handler.setFormatter(logging.Formatter(LOG_FMT, DATE_FMT))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


__VERSION__ = '0.3.13'

__all__ = (
    'Scheduler',
    'MesosSchedulerDriver',
    'Executor',
    'MesosExecutorDriver',
    'encode_data',
    'decode_data',
    'OperatorMaster',
    'MesosOperatorMasterDriver',
    'MesosOperatorAgentDriver',
)
