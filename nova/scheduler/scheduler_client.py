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

from functools import partial

from oslo_log import log as logging

from nova.compute import rpcapi as compute_rpcapi
from nova import exception
from nova.i18n import _LI, _LE, _LW
from nova import objects
from nova.scheduler import cache_manager
from nova.scheduler import claims

LOG = logging.getLogger(__name__)


class APIProxy(cache_manager.APIProxyBase):
    def __init__(self, host):
        super(APIProxy, self).__init__(host)
        self.compute_rpcapi = compute_rpcapi.ComputeAPI()

    def report_host_state(self, context, compute):
        return self.compute_rpcapi.report_host_state(
                context, compute, self.host)


class RemoteCompute(cache_manager.RemoteManagerBase):
    def __init__(self, host, api, manager):
        super(RemoteCompute, self).__init__(host, api, manager)
        # TODO(Yingxin): Apply message pipe and move the according logics to
        # cache_manager.RemoteManagerBase
        self.message_window = cache_manager.MessageWindow(
                label=self.host)
        self.claim_records = cache_manager.ClaimRecords(
                label=self.host)
        self.host_state = None

    def _activate(self, **kwargs):
        seed = kwargs['seed']
        cache = kwargs['cache']
        if self.is_activated() and \
                not self.message_window.try_reset(seed):
            # NOTE(Yingxin): In case multiple caches are coming and they are
            # reordered.
            LOG.warn(_LW("Ignore cache: %s") % cache)
            return
        self.message_window.reset(seed)
        self.host_state = cache
        self.claim_records.reset(
                partial(cache.process_claim, apply_claim=False))

    def _disable(self):
        self.message_window.reset()
        self.host_state = None
        self.claim_records.reset()

    def _refresh(self, context):
        self.api.report_host_state(context, self.host)

    def _do_periodical(self):
        self.claim_records.timeout()
        LOG.info(_LI("Report cache %(host)s: %(state)s")
                 % {'host': self.host, 'state': self})

    def process_commits(self, context, commits, seed):
        # TODO(Yingxin): Overwrite_updates can only handled in sequence, so
        # there should be a way to sort this kind of updates by seed.
        if 'cache_refresh' in commits[0]:
            self.activate(cache=commits[0]['cache_refresh'], seed=seed)
            return

        if not self.expect_active(context):
            return

        try:
            if not self.message_window.proceed(seed):
                LOG.warn(_LW("Ignored message %(msg)s, host %(host)s")
                         % {'msg': commits, 'host': self.host})
                return
        except KeyError:
            self.refresh(context)
            return

        for commit in commits:
            claim_replies = commit['claim_replies']
            for reply in claim_replies:
                if reply.origin_host != self.manager.host:
                    LOG.info(_LI("receive %(instance)s to %(host)s from "
                                 "%(scheduler)s, proceed: %(p)s") %
                             {'instance': reply.instance_uuid,
                              'host': reply.target_host,
                              'scheduler': reply.origin_host,
                              'p': reply.proceed})
                else:
                    tracked_claim = self.claim_records.pop(reply.seed)
                    proceed = reply.proceed
                    if tracked_claim and not proceed:
                        LOG.info(_LI("Failed_ %(instance)s to %(host)s") %
                                 {'instance': tracked_claim.instance_uuid,
                                  'host': tracked_claim.target_host})
                        self.host_state.process_claim(tracked_claim, False)
                        LOG.debug("Updated state: %s" % self.host_state)
                    elif tracked_claim and proceed:
                        LOG.info(_LI("Succeed %(instance)s to %(host)s") %
                                 {'instance': tracked_claim.instance_uuid,
                                  'host': tracked_claim.target_host})
                    else:
                        LOG.warn(_LW("Extra remote reply %(claim)s "
                                     "for instance %(id)s!") %
                                     {'claim': reply,
                                      'id': reply.instance_uuid})

            success = True
            cache_update = commit['cache_update']
            if cache_update:
                success = self.host_state.process_update(cache_update)
                LOG.info(_LI("process update from %(host)s: %(update)s") %
                         {'host': self.host, 'update': cache_update})
                LOG.debug("Updated state: %s" % self)
            if not success:
                LOG.warn(_LW("HostState version doesn't match: update %(u)s,"
                             " actually %(s)s!")
                         % {'u': cache_update, 's': self.host_state})

    def abort_claims(self, claims):
        if not self.is_activated():
            LOG.error(_LE("Abort claims %(claims)s to inactive %(host)s")
                     % {'claims': claims, 'host': self.host})
            return
        for claim in claims:
            tracked_claim = self.claim_records.pop(claim.seed)
            if tracked_claim:
                LOG.info(_LI("Abort claim %s!") % claim)
                self.host_state.process_claim(claim, False)
                LOG.debug("Updated state: %s" % self)
            else:
                LOG.error(_LE("Claim %s not found, abort abort!") % claim)

    def consume_cache(self, spec_obj, limits):
        # TODO(Yingxin): Refactor code to get rid of conductor service when
        # send scheduler decisions to SchedulerServers with claim. That is to
        # say, send decisions directly to SchedulerServers.

        if not self.is_activated():
            raise exception.ComputeResourcesUnavailable(reason=
                    "Remote %s is not available!" % self.host)
        claim = claims.Claim(spec_obj, self.host_state, limits)

        cache_claim = objects.CacheClaim.from_primitives(
                self.seed, self.manager.host, self.host,
                spec_obj.instance_uuid, claim)
        self.increase_seed()
        self.host_state.process_claim(cache_claim, True)
        spec_obj.numa_topology = cache_claim.numa_topology

        # TODO(Yingxin): Also track spec_obj to make the fast-retry possible.
        self.claim_records.track(cache_claim)
        LOG.debug("Successfully consume from claim %(claim)s, "
                  "the state is changed to %(state)s!",
                  {'claim': cache_claim, 'state': self.host_state})

        return cache_claim

    def __repr__(self):
        if not self.is_activated():
            return ("HostState(%s) is in state %s!"
                    % self.host, self.state)
        else:
            return self.host_state.__repr__()


class SchedulerClients(cache_manager.CacheManagerBase):
    API_PROXY = APIProxy
    REMOTE_MANAGER = RemoteCompute
    SERVICE_NAME = 'nova-compute'

    def __init__(self, host):
        super(SchedulerClients, self).__init__(host)

    def receive_commit(self, context, commit, compute, seed):
        LOG.debug("Get commit #%(seed)d from host %(compute)s: %(commit)s.",
                  {"commit": commit, "compute": compute, "seed": seed})
        remote_obj = self._get_remote(compute, "commits")
        remote_obj.process_commits(context, commit, seed)

    def abort_claims(self, claims):
        for claim in claims:
            remote_obj = self._get_remote(claim.target_host, "abort")
            remote_obj.abort_claims([claim])
