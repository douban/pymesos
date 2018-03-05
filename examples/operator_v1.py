#!/usr/bin/env python2.7
from __future__ import print_function

import sys

from pymesos import MesosOperatorMasterDriver, OperatorMaster


class MinimalOperator(OperatorMaster):
    def __init__(self):
        pass

    def taskAdded(self, task_info):
        logging.debug('Task added')
        logging.debug(task_info)

    def taskUpdated(self, task_info):
        logging.debug('Task updated')
        logging.debug(task_info)

    def frameworkAdded(self, framework_info):
        logging.debug('Framework added')
        logging.debug(framework_info)

    def frameworkUpdated(self, framework_info):
        logging.debug('Framework updated')
        logging.debug(framework_info)

    def frameworkRemoved(self, framework_info):
        logging.debug('Framework removed')
        logging.debug(framework_info)

    def agentAdded(self, agent_info):
        logging.debug('Agent added')
        logging.debug(agent_info)

    def agentRemoved(self, agent_info):
        logging.debug('Agent removed')
        logging.debug(agent_info)


def main(master):
    driver = MesosOperatorMasterDriver(master, MinimalOperator())
    res = driver.getHealth()
    print(res)
    driver.run()


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) != 2:
        print("Usage: {} <mesos_master>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1])
