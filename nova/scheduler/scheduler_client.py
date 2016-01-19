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
from nova import servicegroup

LOG = logging.getLogger(__name__)


class APIProxy(object):
    def __init__(self, host):
        self.host = host
        self.servicegroup_api = servicegroup.API()
        self.compute_rpcapi = compute_rpcapi.ComputeAPI()

    def service_is_up(self, service):
        return self.servicegroup_api.service_is_up(service)

    def report_host_state(self, context, compute):
        return self.compute_rpcapi.report_host_state(
                context, compute, self.host)


class SchedulerClients(object):
    def __init__(self, host):
        self.host = host
        self.api = APIProxy(host)
        self.clients = {}
        self.ready_states = {}
        self.seed = random.randint(0, 1000000)

    def periodically_refresh_clients(self, context):
        service_refs = {service.host: service
                        for service in objects.ServiceList.get_by_binary(
                            context, 'nova-compute')}
        service_keys_db = set(service_refs.keys())
        service_keys_cache = set(self.clients.keys())

        new_keys = service_keys_db - service_keys_cache
        old_keys = service_keys_cache - service_keys_db

        for new_key in new_keys:
            client_obj = SchedulerClient(service_refs[new_key].host,
                                         self.api, self)
            self.clients[new_key] = client_obj
            LOG.info(_LI("Added new compute %s from db.") % new_key)

        for old_key in old_keys:
            client_obj = self.clients[old_key]
            if not client_obj.host_state:
                LOG.error(_LE("Remove non-exist compute %s") % old_key)
                del self.clients[old_key]
            else:
                LOG.info(_LE("Keep non-exist compute %s") % old_key)

        for client in self.clients.values():
            if client.host_state:
                LOG.info(_LI("Host state on compute %(compute)s: %(state)s")
                         % {'compute': client.host,
                            'state': client.host_state})

            client.sync(context, service_refs.get(client.host, None))

    def notify_scheduler(self, context, host_name):
        LOG.info(_LI("Get notified from host %s") % host_name)
        client_obj = self.clients.get(host_name, None)
        if not client_obj:
            client_obj = SchedulerClient(host_name, self.api, self)
            self.clients[host_name] = client_obj
            LOG.info(_LI("Added new compute %s from notification.")
                        % host_name)
        client_obj.refresh_state(context, True)

    def send_commit(self, context, commit, compute, seed):
        LOG.info(_LI("Get commit #%(seed)d from host %(compute)s: %(commit)s.")
                 % {"commit": commit, "compute": compute, "seed": seed})
        client_obj = self.clients.get(compute, None)
        if not client_obj:
            client_obj = SchedulerClient(compute, self.api, self)
            self.clients[compute] = client_obj
            LOG.error(_LE("Added new compute %s from commit.")
                        % compute)
        client_obj.process_commit(context, commit, seed)

    def get_all_host_states(self):
        return self.ready_states.values()

    def abort_claims(self, claims):
        for claim in claims:
            host = claim['host']
            client_obj = self.clients.get(host, None)
            if not client_obj:
                LOG.error(_LE("Missing compute %(host)s to abort "
                              "claim %(claim)s!")
                          % {'host': host, 'claim': claim})
                continue
            client_obj.abort_claims([claim])

    def track_claim(self, claim):
        claim['seed'] = self.seed
        self.seed += 1
        client_obj = self.clients.get(claim['host'], None)
        if not client_obj:
            LOG.error(_LE("Missing compute %(host)s to track claim %(claim)s!")
                      % {'host': claim['host'], 'claim': claim})
            return
        client_obj.track_claim(claim)


class SchedulerClient(object):
    def __init__(self, host, api, manager):
        self.host = host
        self.api = api
        self.manager = manager
        # the min window is 1
        self.window_max = 7

        self.host_state = None
        self.tmp = False
        self.seed = None
        self.window = None
        self.claims = None
        self.old_claims = None

    def _handle_tmp(self):
        if self.host_state:
            if self.tmp:
                LOG.info(_LI("Keep compute %s!")
                        % self.host)
                self.tmp = False
            else:
                LOG.info(_LI("Disable compute %s!")
                        % self.host)
                self.disable()
        else:
            self.tmp = False

    def sync(self, context, service):
        if not service:
            LOG.info(_LI("No service entry of compute %s!") % self.host)
            self._handle_tmp()
        elif service['disabled']:
            LOG.info(_LI("Service compute %s is disabled!")
                        % self.host)
            self.disable()
        elif self.api.service_is_up(service):
            self.tmp = False
            if not self.host_state:
                LOG.info(_LI("Compute %s is up without hoststate") % self.host)
                self.refresh_state(context)
            else:
                # normal situation
                timeout_claims = self.old_claims.values()
                if timeout_claims:
                    LOG.error(_LE("Time out claims %s") % timeout_claims)
                    self.abort_claims(timeout_claims)
                self.old_claims = self.claims
                self.claims = {}
                pass
        else:
            self._handle_tmp()

    def refresh_state(self, context, tmp=False):
        LOG.info(_LI("Compute %s is to be refreshed!") % self.host)
        self.api.report_host_state(context, self.host)

    def process_commit(self, context, commit, seed):
        if isinstance(commit, objects.HostState):
            LOG.info(_LI("Compute %s is refreshed!") % self.host)
            self.host_state = commit
            self.seed = seed
            self.manager.ready_states[self.host] = self.host_state
            self.window = []
            self.claims = {}
            self.old_claims = {}
            return

        if self.host_state:
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
                    self.refresh_state(context)
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
                    self.refresh_state(context)
                    return

            success = True
            for item in commit:
                if 'version_expected' in item:
                    success = self.host_state.process_commit(item)
                elif 'instance_uuid' in item:
                    seed = item['seed']
                    instance_uuid = item['instance_uuid']
                    process = item.pop('process', True)
                    if instance_uuid is None:
                        self.host_state.process_claim(item, process)
                    else:
                        in_track = False
                        if seed in self.claims:
                            del self.claims[seed]
                            in_track = True
                        if seed in self.old_claims:
                            del self.old_claims[seed]
                            in_track = True

                        if in_track and not process:
                            LOG.info(_LI("Decision failure for instance %s!")
                                     % instance_uuid)
                            self.host_state.process_claim(item, process)
                        elif in_track and process:
                            LOG.info(_LI("Decision success for instance %s!")
                                     % instance_uuid)
                        elif not in_track and not process:
                            LOG.error(_LE("Outdated decision failure for "
                                          "instance %s!") % instance_uuid)
                        else:
                            LOG.error(_LE("Outdated decision success for "
                                          "instance %s!") % instance_uuid)
                            self.host_state.process_claim(item, process)
                else:
                    LOG.error(_LE("Unable to handle commit %s!") % item)
            if not success:
                LOG.info(_LI("Warn: HostState doesn't match."))
            else:
                LOG.info(_LI("Updated state: %s") % self.host_state)
        else:
            LOG.info(_LE("The commit comes without hoststate"))
            self.refresh_state(context, True)

    def abort_claims(self, claims):
        for claim in claims:
            do_abort = False
            if claim['seed'] in self.claims:
                del self.claims[claim['seed']]
                do_abort = True
            if claim['seed'] in self.old_claims:
                del self.old_claims[claim['seed']]
                do_abort = True

            if do_abort:
                LOG.info(_LI("Abort claim %s!") % claim)
                self.host_state.process_claim(claim, False)
            else:
                LOG.error(_LE("Claim %s not found, abort abort!") % claim)

    def track_claim(self, claim):
        self.claims[claim['seed']] = claim

    def disable(self):
        self.host_state = None
        self.tmp = False
        self.seed = None
        self.window = None
        self.manager.ready_states.pop(self.host, None)
        self.claims = None
        self.old_claims = None
