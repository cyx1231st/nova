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

from oslo_log import log as logging

from nova.i18n import _LI, _LE, _LW
from nova import objects
from nova.scheduler import client as scheduler_client

LOG = logging.getLogger(__name__)


class SchedulerServers(object):
    def __init__(self, host):
        self.servers = {}
        self.host_state = None
        self.host = host
        self.scheduler_api = scheduler_client.SchedulerClient()

    def update_from_compute(self, context, compute):
        if not self.host_state:
            self.host_state = objects.HostState.from_primitives(
                    context, compute)
            self.scheduler_api.notify_schedulers(context, self.host)
            LOG.info(_LI("Scheduler server %s is up!") % self.host)
        else:
            # TODO() incremental update
            pass

    def report_host_state(self, client, server):
        if server != self.host:
            LOG.error(_LE("Message sent to a wrong host"
                          "%(actual)s, expected %(expected)s!"),
                      {'actual': self.host, 'expected': client})
            return
        elif self.disabled:
            LOG.warning(_LW("The host %s isn't ready yet!") % self.host)
            return

        if server not in self.servers:
            server_obj = SchedulerServer()
            self.servers[server] = server_obj
        else:
            server_obj = self.servers[server]

        server_obj.refresh_state()
        return self.host_state

    @property
    def disabled(self):
        return self.host_state is None


class SchedulerServer(object):
    def refresh_state(self):
        pass
