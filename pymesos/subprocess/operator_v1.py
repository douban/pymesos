from .. import OperatorMaster
import logging

logger = logging.getLogger(__name__)

class ProcOperatorMaster(OperatorMaster):

    def __init__(self):
        pass

    def taskAdded(self, task_info):
        logger.debug('Task added: %s' % task_info)

    def taskUpdated(self, task_info):
        logger.debug('Task updated: %s' % task_info)

    def frameworkAdded(self, framework_info):
        logger.debug('Framework added: %s' % framework_info)

    def frameworkUpdated(self, framework_info):
        logger.debug('Framework updated: %s' % framework_info)

    def frameworkRemoved(self, framework_info):
        logger.debug('Framework removed: %s' % framework_info)

    def agentAdded(self, agent_info):
        logger.debug('Agent added: %s' % agent_info)

    def agentRemoved(self, agent_info):
        logger.debug('Agent removed: %s' % agent_info)