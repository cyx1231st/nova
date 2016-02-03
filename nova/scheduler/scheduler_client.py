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

import bisect
import random

from oslo_log import log as logging

from nova.compute import rpcapi as compute_rpcapi
from nova.i18n import _LI, _LE
from nova import objects
from nova.pci import stats as pci_stats
from nova.scheduler import cache_manager

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
        # the min window is 1
        self.window_max = 7

        self.seed = None
        self.window = None
        self.claim_records = cache_manager.ClaimRecords()

        # host state specific
        self.host_state = None
        self.aggregates = []
        self.instances = {}
        self.limits = {}
        
        # TODO(Yingxin): remove after implemented
        self.pci_stats = pci_stats.PciDeviceStats()

    def __getattr__(self, name):
        return getattr(self.host_state, name)

    def consume_from_request(self, spec_obj):
        if not self.is_activated():
            raise RuntimeError("HostState %s is unavailable!" % self.host)
        claim = self.host_state.claim(spec_obj, self.limits)
        claim['seed'] = self.manager.seed
        claim['from'] = self.manager.host
        self.manager.seed += 1

        self.host_state.process_claim(claim, True)
        LOG.debug("Successfully consume from claim %(claim)s, "
                  "the state is changed to %(state)s!",
                  {'claim': claim, 'state': self})
        spec_obj.numa_topology = claim['numa_topology']

        self.claim_records.track(claim['seed'], claim)

        return claim

    def update_from_host_manager(self, context, aggregates, inst_dict):
        if not self.is_activated():
            raise RuntimeError("HostState %s is unavailable" % self.host)
        self.aggregates = aggregates or []
        self.instances = inst_dict or {}

    def __repr__(self):
        if not self.is_activated():
            return ("HostState(%s, %s) is in state %s!"
                    % self.host, self.nodename, self.state)
        return ("HostState(%s, %s) total_usable_ram_mb:%s free_ram_mb:%s "
                "total_usable_disk_gb:%s free_disk_mb:%s disk_mb_used:%s "
                "vcpus_total:%s vcpus_used:%s "
                "numa_topology:%s pci_stats:%s "
                "num_io_ops:%s num_instances:%s" %
                (self.host, self.nodename,
                 self.total_usable_ram_mb, self.free_ram_mb,
                 self.total_usable_disk_gb, self.free_disk_mb,
                 self.disk_mb_used,
                 self.vcpus_total, self.vcpus_used,
                 self.numa_topology, self.pci_stats,
                 self.num_io_ops, self.num_instances))

    def _do_periodical(self):
        self.claim_records.timeout()
        LOG.info(_LI("Report cache %(host)s: %(state)s")
                 % {'host': self.host, 'state': self})

    def _refresh(self, context):
        self.api.report_host_state(context, self.host)

    def _activate(self, cache, seed):
        self.host_state = cache
        self.claim_records.reset(cache)
        self.seed = seed
        self.manager.ready_states[self.host] = self
        self.window = []

    def _disable(self):
        self.host_state = None
        self.seed = None
        self.window = None
        self.manager.ready_states.pop(self.host, None)
        self.claim_records.reset()

    def process_commit(self, context, commit, seed):
        if isinstance(commit, objects.HostState):
            self.activate(commit, seed)
            return

        if not self.expect_active(context):
            return

        # check window
        if seed <= self.seed:
            index = bisect.bisect_left(self.window, seed)
            if seed == self.seed or self.window[index] != seed:
                LOG.error(_LE("Old commit#%d, ignore!") % seed)
                return
            else:
                LOG.info(_LI("A lost commit#%d!") % seed)
                del self.window[index]
        elif seed == self.seed + 1:
            self.seed = seed
        else:
            if seed - self.seed > self.window_max:
                LOG.error(_LE("A gient gap between %(from)d and %(to)d, "
                    "refresh state!") % {'from': self.seed, 'to': seed})
                self.refresh(context)
                return
            else:
                for i in range(self.seed + 1, seed):
                    self.window.append(i)
            self.seed = seed

        if self.window:
            LOG.info(_LI("Missing commits: %s.") % self.window)
            if self.seed - self.window[0] >= self.window_max:
                LOG.error(_LE("Lost exceed window capacity %d, abort!")
                          % self.window_max)
                self.refresh(context)
                return

        success = True
        for item in commit:
            if 'version_expected' in item:
                success = self.host_state.process_commit(item)
                LOG.info(_LI("process commit from %(host)s: %(commit)s") %
                         {'host': self.host, 'commit': item})
                LOG.debug("Updated state: %s" % self)
            elif 'instance_uuid' in item:
                seed = item['seed']
                instance_uuid = item['instance_uuid']
                proceed = item.pop('proceed', True)
                if item['from'] != self.manager.host:
                    LOG.info(_LI("receive %(instance)s to %(host)s from "
                                 "%(scheduler)s") %
                             {'instance': instance_uuid,
                              'host': item['host'],
                              'scheduler': item['from']})
                    self.host_state.process_claim(item, proceed)
                    LOG.debug("Updated state: %s" % self)
                else:
                    tracked_claim = self.claim_records.pop(seed)

                    if tracked_claim and not proceed:
                        LOG.info(_LI("Failed_ %(instance)s to %(host)s") %
                                 {'instance': instance_uuid,
                                  'host': item['host']})
                        self.host_state.process_claim(item, False)
                        LOG.debug("Updated state: %s" % self)
                    elif tracked_claim and proceed:
                        LOG.info(_LI("Succeed %(instance)s to %(host)s") %
                                 {'instance': instance_uuid,
                                  'host': item['host']})
                    else:
                        LOG.error(_LE("Outdated decision %(claim) "
                                      "for instance %(id)s!") % 
                                      {'claim': item,
                                       'id': instance_uuid})
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
            tracked_claim = self.claim_records.pop(claim['seed'])
            if tracked_claim:
                LOG.info(_LI("Abort claim %s!") % claim)
                self.host_state.process_claim(claim, False)
                LOG.debug("Updated state: %s" % self)
            else:
                LOG.error(_LE("Claim %s not found, abort abort!") % claim)


class SchedulerClients(cache_manager.CacheManagerBase):
    API_PROXY = APIProxy
    REMOTE_MANAGER = SharedHostState
    SERVICE_NAME = 'nova-compute'

    def __init__(self, host):
        super(SchedulerClients, self).__init__(host)
        self.ready_states = {}
        self.seed = random.randint(0, 1000000)

    def receive_commit(self, context, commit, compute, seed):
        LOG.debug("Get commit #%(seed)d from host %(compute)s: %(commit)s.",
                  {"commit": commit, "compute": compute, "seed": seed})
        remote_obj = self._get_remote(compute, "commits")
        remote_obj.process_commit(context, commit, seed)

    def get_all_host_states(self):
        return self.ready_states.values()

    def abort_claims(self, claims):
        for claim in claims:
            host = claim['host']
            remote_obj = self._get_remote(host, "abort")
            remote_obj.abort_claims([claim])
