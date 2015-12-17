import json
import time
import logging
import struct
import socket

from mesos.interface.mesos_pb2 import TASK_LOST, MasterInfo

from .messages_pb2 import (
    RegisterFrameworkMessage, ReregisterFrameworkMessage,
    DeactivateFrameworkMessage, UnregisterFrameworkMessage,
    ResourceRequestMessage, ReviveOffersMessage, LaunchTasksMessage, KillTaskMessage,
    StatusUpdate, StatusUpdateAcknowledgementMessage, FrameworkToExecutorMessage,
    ReconcileTasksMessage
)
from .process import UPID, Process, async


logger = logging.getLogger(__name__)


class MesosSchedulerDriver(Process):
    def __init__(self, sched, framework, master_uri):
        Process.__init__(self, 'scheduler')
        self.sched = sched
        #self.executor_info = executor_info
        self.master_uri = master_uri
        self.framework = framework
        self.framework.failover_timeout = 100
        self.framework_id = framework.id

        self.master = None
        self.detector = None

        self.connected = False
        self.savedOffers = {}
        self.savedSlavePids = {}

    @async # called by detector
    def onNewMasterDetectedMessage(self, data):
        master = None
        try:
            parsed = json.loads(data)
            if parsed and "address" in parsed:
                ip = parsed["address"].get("ip")
                port = parsed["address"].get("port")
                if ip and port:
                    master = UPID("master@%s:%s" % (ip, port))
        except ValueError as parse_error:
            logger.debug("No JSON content, probably connecting "
                         "to older Mesos version. Reason: %s", parse_error)
        if not master:
            try:
                info = MasterInfo()
                info.ParseFromString(data)
                ip = socket.inet_ntoa(struct.pack('<I', info.ip))
                master = UPID('master@%s:%s' % (ip, info.port))
            except:
                master = UPID(data)

        self.connected = False
        self.register(master)

    @async # called by detector
    def onNoMasterDetectedMessage(self):
        self.connected = False
        self.master = None

    def register(self, master):
        if self.connected or self.aborted:
            return

        if master:
            if not self.framework_id.value:
                msg = RegisterFrameworkMessage()
                msg.framework.MergeFrom(self.framework)
            else:
                msg = ReregisterFrameworkMessage()
                msg.framework.MergeFrom(self.framework)
                msg.failover = True
            self.send(master, msg)

        self.delay(2, lambda:self.register(master))

    def onFrameworkRegisteredMessage(self, framework_id, master_info):
        self.framework_id = framework_id
        self.framework.id.MergeFrom(framework_id)
        self.connected = True
        self.master = UPID('master@%s:%s' % (socket.inet_ntoa(struct.pack('<I', master_info.ip)), master_info.port))
        self.link(self.master, self.onDisconnected)
        self.sched.registered(self, framework_id, master_info)

    def onFrameworkReregisteredMessage(self, framework_id, master_info):
        assert self.framework_id == framework_id
        self.connected = True
        self.master = UPID('master@%s:%s' % (socket.inet_ntoa(struct.pack('<I', master_info.ip)), master_info.port))
        self.link(self.master, self.onDisconnected)
        self.sched.reregistered(self, master_info)

    def onDisconnected(self):
        self.connected = False
        logger.warning("disconnected from master")
        self.delay(5, lambda:self.register(self.master))

    def onResourceOffersMessage(self, offers, pids):
        for offer, pid in zip(offers, pids):
            self.savedOffers.setdefault(offer.id.value, {})[offer.slave_id.value] = UPID(pid)
        self.sched.resourceOffers(self, list(offers))

    def onRescindResourceOfferMessage(self, offer_id):
        self.savedOffers.pop(offer_id.value, None)
        self.sched.offerRescinded(self, offer_id)

    def onStatusUpdateMessage(self, update, pid=''):
        if self.sender.addr != self.master.addr:
            logger.warning("ignore status update message from %s instead of leader %s", self.sender, self.master)
            return

        assert self.framework_id == update.framework_id

        self.sched.statusUpdate(self, update.status)
        
        if not self.aborted and self.sender.addr and pid:
            reply = StatusUpdateAcknowledgementMessage()
            reply.framework_id.MergeFrom(self.framework_id)
            reply.slave_id.MergeFrom(update.slave_id)
            reply.task_id.MergeFrom(update.status.task_id)
            reply.uuid = update.uuid
            try: self.send(self.master, reply)
            except IOError: pass


    def onLostSlaveMessage(self, slave_id):
        self.sched.slaveLost(self, slave_id)

    def onExecutorToFrameworkMessage(self, slave_id, framework_id, executor_id, data):
        self.sched.frameworkMessage(self, executor_id, slave_id, data)

    def onFrameworkErrorMessage(self, message, code=0):
        self.sched.error(self, message)

    def onExitedExecutorMessage(self, slave_id, framework_id, executor_id, status):
        logger.warning("Executor %s exited with status %s.", executor_id.value, status)
        self.sched.executorLost(self, executor_id, slave_id, status)

    def start(self):
        Process.start(self)
        uri = self.master_uri
        if uri.startswith('zk://') or uri.startswith('zoo://'):
            from .detector import MasterDetector
            self.detector = MasterDetector(uri[uri.index('://') + 3:], self)
            self.detector.start()
        else:
            if not ':' in uri:
                uri += ':5050'
            self.onNewMasterDetectedMessage('master@%s' % uri)

    def abort(self):
        if self.connected:
            msg = DeactivateFrameworkMessage()
            msg.framework_id.MergeFrom(self.framework_id)
            self.send(self.master, msg)
        Process.abort(self)

    def stop(self, failover=False):
        if self.connected and not failover:
            msg = UnregisterFrameworkMessage()
            msg.framework_id.MergeFrom(self.framework_id)
            self.send(self.master, msg)
        if self.detector:
            self.detector.stop()
        Process.stop(self)

    @async
    def requestResources(self, requests):
        if not self.connected:
            return
        msg = ResourceRequestMessage()
        msg.framework_id.MergeFrom(self.framework_id)
        for req in requests:
            msg.requests.add().MergeFrom(req)
        self.send(self.master, msg)

    @async
    def reviveOffers(self):
        if not self.connected:
            return
        msg = ReviveOffersMessage()
        msg.framework_id.MergeFrom(self.framework_id)
        self.send(self.master, msg)

    @async
    def reconcileTasks(self, statuses=None):
        if not self.connected:
            return
        msg = ReconcileTasksMessage()
        msg.framework_id.MergeFrom(self.framework_id)
        if statuses is not None:
            msg.statuses = statuses
        self.send(self.master, msg)

    def launchTasks(self, offer_id, tasks, filters):
        if not self.connected or offer_id.value not in self.savedOffers:
            for task in tasks:
                update = StatusUpdate()
                update.framework_id.MergeFrom(self.framework_id)
                update.status.task_id.MergeFrom(task.task_id)
                update.status.state = TASK_LOST
                update.status.message = 'Master disconnected' if not self.connected else "invalid offer_id"
                update.timestamp = time.time()
                update.uuid = ''
                self.onStatusUpdateMessage(update)
            return

        msg = LaunchTasksMessage()
        msg.framework_id.MergeFrom(self.framework_id)
        msg.offer_ids.add().MergeFrom(offer_id)
        msg.filters.MergeFrom(filters)
        for task in tasks:
            msg.tasks.add().MergeFrom(task)
            pid = self.savedOffers.get(offer_id.value, {}).get(task.slave_id.value)
            if pid and task.slave_id.value not in self.savedSlavePids:
                self.savedSlavePids[task.slave_id.value] = pid
        self.savedOffers.pop(offer_id.value)
        self.send(self.master, msg)

    def declineOffer(self, offer_id, filters=None):
        if not self.connected:
            return
        msg = LaunchTasksMessage()
        msg.framework_id.MergeFrom(self.framework_id)
        msg.offer_ids.add().MergeFrom(offer_id)
        if filters:
             msg.filters.MergeFrom(filters)
        self.send(self.master, msg)

    @async
    def killTask(self, task_id):
        if not self.connected:
            return
        msg = KillTaskMessage()
        msg.framework_id.MergeFrom(self.framework_id)
        msg.task_id.MergeFrom(task_id)
        self.send(self.master, msg)

    @async
    def sendFrameworkMessage(self, executor_id, slave_id, data):
        if not self.connected:
            return

        msg = FrameworkToExecutorMessage()
        msg.framework_id.MergeFrom(self.framework_id)
        msg.executor_id.MergeFrom(executor_id)
        msg.slave_id.MergeFrom(slave_id)
        msg.data = data

        slave = self.savedSlavePids.get(slave_id.value, self.master) # can not send to slave directly
        self.send(slave, msg)
