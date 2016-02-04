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

import random

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


class SharedHostState(cache_manager.RemoteManagerBase):
    def __init__(self, host, api, manager):
        super(SharedHostState, self).__init__(host, api, manager)
        self.message_window = cache_manager.MessageWindow(
                label=self.host)
        self.claim_records = cache_manager.ClaimRecords(
                label=self.host)
        self.seed = random.randint(0, 1000000)
        self.host_state = None

    def _activate(self, cache, seed):
        if self.is_activated() and \
                not self.message_window.try_reset(seed):
            # NOTE(Yingxin): In case multiple caches are coming and they are
            # reordered.
            LOG.warn(_LW("Ignore cache: %s") % cache)
            return
        self.message_window.reset(seed)
        self.host_state = cache
        self.claim_records.reset(cache)

    def _disable(self):
        self.message_window.reset()
        # TODO(Yingxin): Cannot simply set to None unless scheduler accepts
        # empty host state.
        self.host_state = None
        self.claim_records.reset()

    def _refresh(self, context):
        self.api.report_host_state(context, self.host)

    def _do_periodical(self):
        self.claim_records.timeout()
        LOG.info(_LI("Report cache %(host)s: %(state)s")
                 % {'host': self.host, 'state': self})

    def process_commit(self, context, commit, seed):
        if isinstance(commit, objects.HostState):
            self.activate(commit, seed)
            return

        if not self.expect_active(context):
            return

        try:
            if not self.message_window.proceed(seed):
                return
        except KeyError:
            self.refresh(context)
            return

        success = True
        for item in commit:
            if isinstance(item, objects.CacheClaim):
                claim = item
                proceed = claim.proceed
                if claim.origin_host != self.manager.host:
                    LOG.info(_LI("receive %(instance)s to %(host)s from "
                                 "%(scheduler)s") %
                             {'instance': claim.instance_uuid,
                              'host': claim.target_host,
                              'scheduler': claim.origin_host})
                    self.host_state.process_claim(claim, proceed)
                    LOG.debug("Updated state: %s" % self.host_state)
                else:
                    tracked_claim = self.claim_records.pop(claim.seed)

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
                        LOG.error(_LE("Unrecognized remote claim %(claim) "
                                      "for instance %(id)s!") %
                                      {'claim': claim,
                                       'id': claim.instance_uuid})
            elif 'version_expected' in item:
                success = self.host_state.process_commit(item)
                LOG.info(_LI("process commit from %(host)s: %(commit)s") %
                         {'host': self.host, 'commit': item})
                LOG.debug("Updated state: %s" % self)
            else:
                LOG.error(_LE("Unable to handle commit %s!") % item)
        if not success:
            LOG.info(_LI("HostState doesn't match."))

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
        if not self.is_activated():
            raise exception.ComputeResourcesUnavailable(reason=
                    "Remote %s is not available!" % self.host)
        claim = claims.Claim(spec_obj, self.host_state, limits)

        cache_claim = objects.CacheClaim.from_primitives(
                self.seed, self.manager.host, self.host,
                spec_obj.instance_uuid, claim)
        self.seed += 1
        self.host_state.process_claim(cache_claim, True)
        spec_obj.numa_topology = cache_claim.numa_topology
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
    REMOTE_MANAGER = SharedHostState
    SERVICE_NAME = 'nova-compute'

    def __init__(self, host):
        super(SchedulerClients, self).__init__(host)

    def receive_commit(self, context, commit, compute, seed):
        LOG.debug("Get commit #%(seed)d from host %(compute)s: %(commit)s.",
                  {"commit": commit, "compute": compute, "seed": seed})
        remote_obj = self._get_remote(compute, "commits")
        remote_obj.process_commit(context, commit, seed)

    def abort_claims(self, claims):
        for claim in claims:
            remote_obj = self._get_remote(claim.target_host, "abort")
            remote_obj.abort_claims([claim])
