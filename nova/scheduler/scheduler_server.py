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

from oslo_log import log as logging

import nova
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

    def notify_schduler(self, context, scheduler):
        return self.scheduler_api.notify_scheduler(
                context, self.host, scheduler)

    def send_commit(self, context, commit, scheduler):
        return self.scheduler_api.send_commit(
                context, commit, self.host, scheduler)


class SchedulerServers(object):
    def __init__(self, host):
        self.servers = {}
        self.host_state = None
        self.host = host
        self.api = APIProxy(host)

    def update_from_compute(self, context, compute):
        if not self.host_state:
            self.host_state = objects.HostState.from_primitives(
                    context, compute)
            self.api.notify_schedulers(context)
            LOG.info(_LI("Scheduler server %s is up!") % self.host)
        else:
            commit = self.host_state.update_from_compute(context, compute)
            if commit:
                for server in self.servers.values():
                    if server.queue is not None:
                        server.queue.put(commit)

    def report_host_state(self, compute, scheduler):
        if compute != self.host:
            LOG.error(_LE("Message sent to a wrong host"
                          "%(actual)s, expected %(expected)s!"),
                      {'actual': self.host, 'expected': compute})
            return
        elif not self.host_state:
            LOG.error(_LW("The host %s isn't ready yet!") % self.host)
            return

        server_obj = self.servers.get(scheduler, None)
        if not server_obj:
            server_obj = SchedulerServer(scheduler, self.api)
            self.servers[scheduler] = server_obj
            LOG.info(_LW("Added temp server %s from report.") % scheduler)

        server_obj.refresh_state()
        return self.host_state

    def periodically_refresh_servers(self, context):
        service_refs = {service.host: service
                        for service in objects.ServiceList.get_by_binary(
                            context, 'nova-scheduler')}
        service_keys_db = set(service_refs.keys())
        service_keys_cache = set(self.servers.keys())

        new_keys = service_keys_db - service_keys_cache
        old_keys = service_keys_cache - service_keys_db

        for new_key in new_keys:
            server_obj = SchedulerServer(service_refs[new_key].host, self.api)
            self.servers[new_key] = server_obj
            LOG.info(_LI("Added new server: %s") % new_key)

        for old_key in old_keys:
            server_obj = self.servers[old_key]
            if server_obj.queue is None:
                LOG.error(_LI("Remove server: %s") % old_key)
                del self.servers[old_key]

        for server in self.servers.values():
            server.sync(context, service_refs.get(server.host, None))


class SchedulerServer(object):
    def __init__(self, host, api):
        self.host = host
        self.queue = None
        self.api = api
        self.tmp = False
        self.thread = None

    def _handle_tmp(self):
        if self.queue is not None:
            if self.tmp:
                LOG.info(_LI("Keep service nova-scheduler %s!")
                         % self.host)
                self.tmp = False
            else:
                LOG.info(_LI("Service nova-scheduler %s is disabled!")
                         % self.host)
                self.disable()
        else:
            self.tmp = False

    def sync(self, context, service):
        if not service:
            LOG.info(_LI("No db entry of nova-scheduler %s!") % self.host)
            self._handle_tmp()
        elif service['disabled']:
            LOG.info(_LI("Service nova-scheduler %s is disabled!")
                     % self.host)
            self.disable()
        elif self.api.service_is_up(service):
            self.tmp = False
            if self.queue is None:
                self.api.notify_scheduler(context, self.host)
        else:
            self._handle_tmp()

    def refresh_state(self):
        LOG.info(_LI("scheduler %s is refreshed!") % self.host)
        self.disable()

        self.tmp = True
        self.queue = queue.Queue()
        self.thread = utils.spawn(
            self._dispatch_commits, nova.context.get_admin_context())

    def _dispatch_commits(self, context):
        while True:
            jobs = []
            jobs.append(self.queue.get())
            for i in range(self.queue.qsize(), 0, -1):
                jobs.append(self.queue.get_nowait())
            LOG.info(_LI("Send commits to %(scheduler)s: %(commit)s")
                    % {'scheduler': self.host, 'commit': jobs})
            self.api.send_commit(context, jobs, self.host)

    def disable(self):
        self.tmp = False
        if self.thread:
            self.thread.kill()
        self.queue = None