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

import copy

from oslo_log import log as logging

from nova import exception
from nova.i18n import _LI, _LE, _LW
from nova import objects
from nova.scheduler import cache_manager
from nova.scheduler import client as scheduler_client

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


# TODO: Get rid of this class to make compute node side stateless
class RemoteScheduler(cache_manager.RemoteManagerBase):
    def __init__(self, host, api, manager):
        super(RemoteScheduler, self).__init__(host, api, manager)
        # TODO(Yingxin): Apply message window to detect and recover lost
        # claims, then move message_window to RemoteManagerBase.
        self.message_pipe = cache_manager.MessagePipe(
            self._dispatch_commits, True, label=self.host)

    def _activate(self, **kwargs):
        self.message_pipe.activate("refresh")

    def _disable(self):
        self.message_pipe.disable()

    def _refresh(self, context):
        if self.manager.host_state:
            self.activate()

    def _dispatch_commits(self, context, messages):
        if not self.manager.host_state:
            LOG.error(_LE("Host state is not available, abort dispatching!"))
            self.disable()

        if messages[0] == "refresh":
            LOG.info(_LI("Scheduler %(host)s is refreshed by %(seed)d!")
                     % {'host': self.host, 'seed': self.seed})
            cache_commit = cache_manager.build_commit_from_cache(
                self.manager.host_state)
            self.api.send_commit(context, [cache_commit],
                                 self.host, self.seed)
        else:
            # NOTE(Yingxin): Do not modify any messages, they are shared
            # between RemoteScheduler objects!
            cache_commit = copy.deepcopy(messages[0])
            for i in range(1, len(messages)):
                cache_manager.merge_commit(cache_commit, messages[i])
            LOG.info(_LI("Send %(count)s commit#%(seed)d to %(scheduler)s: "
                         "%(commit)s")
                     % {'scheduler': self.host,
                        'commit': cache_commit,
                        'seed': self.seed,
                        'count': len(messages)})
            self.api.send_commit(context, [cache_commit], self.host, self.seed)
        self.increase_seed()

    def reply_claim(self, context, claim, proceed, force=False):
        if not self.is_activated():
            return
        claim_reply = objects.ClaimReply.from_claim(claim, proceed)
        if not force and claim.origin_host == self.host:
            cache_update = None
        else:
            cache_update = claim.to_cache_update(proceed)
        cache_commit = cache_manager.build_commit(claim_reply=claim_reply,
                                                  cache_update=cache_update)
        self.send_commit(context, cache_commit)

    def send_commit(self, context, commit):
        if not self.is_activated():
            return
        self.message_pipe.put(commit)


class SchedulerServers(cache_manager.CacheManagerBase):
    API_PROXY = APIProxy
    REMOTE_MANAGER = RemoteScheduler
    SERVICE_NAME = 'nova-scheduler'

    def __init__(self, host):
        super(SchedulerServers, self).__init__(host)
        self.host_state = None

    # This is also called periodical from resource tracker to sync the
    # host_state from scheduler.
    def update_from_compute(self, context, compute, claim, proceed):
        if not self.host_state:
            self.host_state = objects.HostState.from_primitives(
                context, compute)
            # Force update schedulers
            self.api.notify_schedulers(context)
            LOG.info(_LI("Compute %s is up!") % self.host)
        else:
            if claim and proceed:
                # NOTE(Yingxin): It should be guaranteed to produce no
                # cache_update if this condition is put after
                # update_from_compute()
                return

            cache_update = self.host_state.update_from_compute(context,
                    compute)
            if cache_update:
                if claim and not proceed:
                    LOG.info(_LI("Instance build aborted, send reversed "
                                 "update. Related claim: %s") % claim)
                LOG.info(_LI("Host state update: %s") % cache_update)
                cache_commit = cache_manager.build_commit(
                        cache_update=cache_update)
                # TODO: fanout
                for remote in self.get_active_managers():
                    remote.send_commit(context, cache_commit)

    def _do_periodical(self):
        LOG.info(_LI("Scheduler host state: %s") % self.host_state)

    def _accept_claim(self, context, claim):
        if not claim:
            # Decision from a legacy filter scheduler
            return
        if claim.seed == -1:
            raise RuntimeError("This claim reply has been sent! %s" % claim)
        self.host_state.process_claim(claim, True)
        # TODO: fanout
        for remote in self.get_active_managers():
            remote.reply_claim(context, claim, True)
        claim.seed = -1

    def _reject_claim(self, context, claim):
        if not claim:
            # Decision from a legacy filter scheduler
            return
        if not self.host_state:
            LOG.warn(_LW("Host state is not ready, ignore claim: %s")
                     % claim)
            return
        if claim.seed == -1:
            raise RuntimeError("This claim reply has been sent! %s" % claim)
        remote_obj = self._get_remote(claim.origin_host, "claim")
        remote_obj.expect_active(context)
        LOG.info(_LI("Fail scheduler %(scheduler)s claim: %(claim)s!")
                 % {'scheduler': claim.origin_host, 'claim': claim})
        remote_obj.reply_claim(context, claim, False)
        claim.seed = -1

    def handle_rt_claim_failure(self, claim, func, *args, **kwargs):
        # Make sure the claim is replied and only once
        context = args[1]
        try:
            ret = func(*args, **kwargs)
        except exception.ComputeResourcesUnavailable as e:
            # failed, abort claim
            self._reject_claim(context, claim)
            raise e
        except Exception as e:
            LOG.error(_LW("Caught an unexpected exception (%s). It is a bug, "
                          "abort claim!") % e)
            self._reject_claim(context, claim)
            raise e
        else:
            self._accept_claim(context, claim)
            return ret
