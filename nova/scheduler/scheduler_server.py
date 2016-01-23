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
from nova.scheduler import client as scheduler_client
from nova import servicegroup
from nova import utils

LOG = logging.getLogger(__name__)


class APIProxy(object):
    def __init__(self, host):
        self.host = host
        self.servicegroup_api = servicegroup.API()
        self.scheduler_api = scheduler_client.SchedulerClient()

    def service_is_up(self, service):
        return self.servicegroup_api.service_is_up(service)

    def notify_schedulers(self, context):
        return self.scheduler_api.notify_schedulers(context, self.host)

    def notify_scheduler(self, context, scheduler):
        return self.scheduler_api.notify_scheduler(
                context, self.host, scheduler)

    def send_commit(self, context, commit, scheduler, seed):
        return self.scheduler_api.send_commit(
                context, commit, self.host, scheduler, seed)


class SchedulerServers(object):
    def __init__(self, host):
        self.servers = {}
        self.host_state = None
        self.compute_state = None
        self.host = host
        self.api = APIProxy(host)

        self.claims = {}
        self.old_claims = {}

    def claim(self, claim, limits):
        try:
            self.host_state.claim(claim, limits)
        except exception.ComputeResourcesUnavailable as e:
            self._fail_claim(claim)
            raise e

        self._success_claim(claim)

    def _fail_claim(self, claim):
            LOG.info(_LI("Fail scheduler %(scheduler)s claim: %(claim)s!")
                     % {'scheduler': claim['from'], 'claim': claim})
            server_obj = self.servers.get(claim['from'], None)
            if server_obj:
                server_obj.send_claim(claim, False)
            else:
                LOG.error(_LE("Cannot abort claim because scheduer %s is "
                              "unavailable!") % claim['from'])

    def _success_claim(self, claim):
        LOG.info(_LI("Success scheduler %(scheduler)s claim: %(claim)s!")
                 % {'scheduler': claim['from'], 'claim': claim})
        self.host_state.process_claim(claim, True)
        self.claims[claim['seed']] = claim
        for server in self.servers.values():
            server.send_claim(claim, True)

    def update_from_compute(self, context, compute, claim, proceed):
        if not self.host_state:
            self.host_state = objects.HostState.from_primitives(
                    context, compute)
            self.compute_state = self.host_state.obj_clone()
            self.api.notify_schedulers(context)
            LOG.info(_LI("Compute %s is up!") % self.host)
        else:
            if claim:
                do_proceed = False
                if claim['seed'] in self.claims:
                    del self.claims[claim['seed']]
                    do_proceed = True
                if claim['seed'] in self.old_claims:
                    del self.claims[claim['seed']]
                    do_proceed = True
                if do_proceed:
                    if proceed:
                        self.compute_state.process_claim(claim, True)
                        LOG.info(_LI("Compute claim success: %s") % claim)
                    else:
                        LOG.info(_LI("Compute claim failed: %s") % claim)
                else:
                    LOG.error(_LE("Unrecognized compute claim: %s") % claim)

            commit = self.compute_state.update_from_compute(context, compute)
            if commit:
                if claim:
                    LOG.warn(_LW("EXTRA COMMIT!"))
                LOG.info(_LI("Host state change: %s") % claim)
                for server in self.servers.values():
                    if server.queue is not None:
                        server.queue.put_nowait(commit)

    def report_host_state(self, compute, scheduler):
        if compute != self.host:
            LOG.error(_LE("Message sent to a wrong host"
                          "%(actual)s, expected %(expected)s!"),
                      {'actual': self.host, 'expected': compute})
            return
        # elif not self.host_state:
        #    LOG.error(_LW("The host %s isn't ready yet!") % self.host)
        #    return

        server_obj = self.servers.get(scheduler, None)
        if not server_obj:
            server_obj = SchedulerServer(scheduler, self.api, self)
            self.servers[scheduler] = server_obj
            LOG.info(_LW("Added new scheduler %s from request.") % scheduler)

        server_obj.refresh_state()
        return
        # return self.host_state, server_obj.seed

    def periodically_refresh_servers(self, context):
        LOG.info(_LI("Report host state: %s") % self.host_state)
        service_refs = {service.host: service
                        for service in objects.ServiceList.get_by_binary(
                            context, 'nova-scheduler')}
        service_keys_db = set(service_refs.keys())
        service_keys_cache = set(self.servers.keys())

        new_keys = service_keys_db - service_keys_cache
        old_keys = service_keys_cache - service_keys_db

        for new_key in new_keys:
            server_obj = SchedulerServer(
                    service_refs[new_key].host, self.api, self)
            self.servers[new_key] = server_obj
            LOG.info(_LI("Added new scheduler %s from db.") % new_key)

        for old_key in old_keys:
            server_obj = self.servers[old_key]
            if server_obj.queue is None:
                LOG.error(_LI("Remove non-exist scheduler %s") % old_key)
                del self.servers[old_key]
            else:
                LOG.info(_LI("Keep non-exist scheduler %s") % old_key)

        for server in self.servers.values():
            server.sync(context, service_refs.get(server.host, None))

        if self.host_state:
            timeout_claims = self.old_claims.values()
            if timeout_claims:
                LOG.error(_LE("Abort timeout claims %s") % timeout_claims)
                for claim in timeout_claims:
                    self.host_state.process_claim(claim, False)
            self.old_claims = self.claims
            self.claims = {}


class SchedulerServer(object):
    def __init__(self, host, api, manager):
        self.host = host
        self.manager = manager
        self.queue = None
        self.api = api
        self.tmp = False
        self.thread = None
        self.seed = random.randint(0, 1000000)
        LOG.info(_LI("Seed %d") % self.seed)

    def _handle_tmp(self):
        if self.queue is not None:
            if self.tmp:
                LOG.info(_LI("Keep scheduler %s!")
                         % self.host)
                self.tmp = False
            else:
                LOG.info(_LI("Disable scheduler %s!")
                         % self.host)
                self.disable()
        else:
            self.tmp = False

    def send_claim(self, claim, proceed):
        if self.queue is not None:
            claim['proceed'] = proceed
            self.queue.put_nowait(claim)

    def sync(self, context, service):
        if not service:
            LOG.info(_LI("No service entry of scheduler %s!") % self.host)
            self._handle_tmp()
        elif service['disabled']:
            LOG.info(_LI("Service scheduler %s is disabled!")
                     % self.host)
            self.disable()
        elif self.api.service_is_up(service):
            if self.manager.host_state:
                self.tmp = False
                if self.queue is None:
                    self.api.notify_scheduler(context, self.host)
        else:
            self._handle_tmp()

    def refresh_state(self):
        LOG.info(_LI("Scheduler %s is to be refreshed!") % self.host)
        self.disable()

        self.tmp = True
        self.queue = queue.Queue()
        self.queue.put_nowait("refresh")
        self.thread = utils.spawn(
            self._dispatch_commits, nova.context.get_admin_context())

    def _dispatch_commits(self, context):
        while True:
            if not self.manager.host_state:
                LOG.error(_LE("No host_state available in %s, abort!")
                          % self.host)
                self.disable()

            if not self.queue:
                LOG.error(_LE("No queue available in %s, abort!")
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

    def disable(self):
        self.tmp = False
        self.queue = None
        # this must be the last one
        if self.thread:
            self.thread.kill()
