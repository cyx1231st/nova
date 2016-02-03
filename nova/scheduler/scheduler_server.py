# Copyright (c) 2016 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
from eventlet import queue
import random

from oslo_log import log as logging

import nova
from nova import exception
from nova.i18n import _LI, _LE, _LW
from nova import objects
from nova.scheduler import cache_manager
from nova.scheduler import client as scheduler_client
from nova import servicegroup
from nova import utils

LOG = logging.getLogger(__name__)


class APIProxy(cache_manager.APIProxyBase):
    def __init__(self, host):
        super(APIProxy, self).__init__(host)
        self.scheduler_api = scheduler_client.SchedulerClient()

    def notify_schedulers(self, context, scheduler=None):
        return self.scheduler_api.notify_schedulers(
                context, self.host, scheduler)

    def send_commit(self, context, commit, scheduler, seed):
        return self.scheduler_api.send_commit(
                context, commit, self.host, scheduler, seed)


class SchedulerServer(cache_manager.RemoteManagerBase):
    def __init__(self, host, api, manager):
        super(SchedulerServer, self).__init__(host, api, manager)
        self.queue = None
        self.thread = None
        self.seed = random.randint(0, 1000000)
        LOG.info(_LI("Seed %d") % self.seed)

    def send_claim(self, context, claim, proceed):
        self.expect_active(context)
        claim['proceed'] = proceed
        self.queue.put_nowait(claim)

    def send_commit(self, context, commit):
        self.expect_active(context)
        self.queue.put_nowait(commit)

    def _activate(self, item, seed):
        self.queue = queue.Queue()
        self.queue.put_nowait("refresh")
        if self.thread:
            self.thread.kill()
        self.thread = utils.spawn(
            self._dispatch_commits, nova.context.get_admin_context())

    def _refresh(self, context):
        if self.manager.host_state:
            self.activate()

    def _disable(self):
        self.queue = None

        # NOTE(Yingxin): Kill must be executed at last, because _disable could
        # be executed by self.thread
        if self.thread:
            self.thread.kill()

    def _dispatch_commits(self, context):
        while True:
            if not self.manager.host_state:
                LOG.error(_LE("No host_state available in %s, abort!")
                          % self.host)
                self.disable()

            if not self.is_activated():
                LOG.error(_LE("Remote %s not activated, abort!")
                          % self.host)
                self.disable()

            jobs = []
            jobs.append(self.queue.get())
            for i in range(self.queue.qsize(), 0, -1):
                jobs.append(self.queue.get_nowait())

            self.seed = self.seed + 1
            if jobs[0] == "refresh":
                LOG.info(_LI("Scheduler %(host)s is refreshed by %(seed)d!")
                         % {'host': self.host, 'seed': self.seed})
                self.api.send_commit(context, self.manager.host_state,
                                     self.host, self.seed)
            else:
                LOG.info(_LI("Send commit#%(seed)d to %(scheduler)s: "
                             "%(commit)s")
                         % {'scheduler': self.host,
                            'commit': jobs,
                            'seed': self.seed})
                self.api.send_commit(context, jobs, self.host, self.seed)


class SchedulerServers(cache_manager.CacheManagerBase):
    API_PROXY = APIProxy
    REMOTE_MANAGER = SchedulerServer
    SERVICE_NAME = 'nova-scheduler'

    def __init__(self, host):
        super(SchedulerServers, self).__init__(host)
        self.host_state = None
        self.compute_state = None
        self.claim_records = cache_manager.ClaimRecords()

    def claim(self, context, claim, limits):
        try:
            self.host_state.claim(claim, limits)
        except exception.ComputeResourcesUnavailable as e:
            self._fail_claim(context, claim)
            raise e

        self._success_claim(context, claim)

    def _fail_claim(self, context, claim):
        host = claim['from']
        LOG.info(_LI("Fail scheduler %(scheduler)s claim: %(claim)s!")
                 % {'scheduler': host, 'claim': claim})
        remote_obj = self._get_remote(host, "claim_fail")
        remote_obj.send_claim(context, claim, False)

    def _success_claim(self, context, claim):
        host = claim['from']
        LOG.info(_LI("Success scheduler %(scheduler)s claim: %(claim)s!")
                 % {'scheduler': host, 'claim': claim})
        self.host_state.process_claim(claim, True)
        self.claim_records.track(claim['seed'], claim)
        for remote in self.remotes.values():
            remote.send_claim(context, claim, True)

    def update_from_compute(self, context, compute, claim, proceed):
        if not self.host_state:
            self.host_state = objects.HostState.from_primitives(
                    context, compute)
            self.claim_records.reset(self.host_state)
            self.compute_state = self.host_state.obj_clone()
            self.api.notify_schedulers(context)
            LOG.info(_LI("Compute %s is up!") % self.host)
        else:
            if claim:
                tracked_claim = self.claim_records.pop(claim['seed'])
                if tracked_claim:
                    if proceed:
                        LOG.info(_LI("Compute claim success: %s")
                                 % tracked_claim)
                        self.compute_state.process_claim(tracked_claim, True)
                    else:
                        LOG.info(_LI("Compute claim failed: %s")
                                 % tracked_claim)
                else:
                    LOG.error(_LE("Unrecognized compute claim: %s") % claim)

            commit = self.compute_state.update_from_compute(context, compute)
            if commit:
                if claim:
                    LOG.warn(_LW("EXTRA COMMIT!"))
                LOG.info(_LI("Host state change: %s") % commit)
                self.host_state.process_commit(commit)
                for remote in self.remotes.values():
                    remote.send_commit(context, commit)

    def _do_periodical(self):
        if self.host_state:
            self.claim_records.timeout()
            LOG.info(_LI("Report cache: %s") % self.host_state)
